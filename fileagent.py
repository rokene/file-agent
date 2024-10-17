import os
import io
import pickle
import re
import json
import logging
import ssl
import traceback
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Tuple

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from logging.handlers import RotatingFileHandler
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from googleapiclient.errors import HttpError
from tqdm import tqdm
import threading

# Define the scopes for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Set up rotating logging
def setup_logging(log_file: str = 'file_download.log') -> logging.Logger:
    handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
    logging.basicConfig(
        handlers=[handler],
        level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger()

logger = setup_logging()

def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """
    Sanitize the filename to remove invalid characters and shorten the length.
    """
    if filename is None:
        return "Unnamed_File"
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    return sanitized[:max_length] if len(sanitized) > max_length else sanitized

@retry(
    retry=retry_if_exception_type((ConnectionResetError, OSError, HttpError, ssl.SSLError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def list_files_in_folder(service: Any, folder_id: str) -> List[Dict[str, Any]]:
    """
    List all files and folders in a specified Google Drive folder with retry on errors.
    """
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        page_token = None
        items = []
        while True:
            response = service.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()
            items.extend(response.get('files', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        logger.info(f"Found {len(items)} items in folder ID {folder_id}.")
        return items
    except HttpError as e:
        logger.error(f"Error listing files in folder {folder_id}: {e}")
        logger.error(traceback.format_exc())
        raise

@retry(
    retry=retry_if_exception_type((ConnectionResetError, OSError, ssl.SSLError, HttpError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def download_file(
    service: Any,
    file_id: str,
    file_name: str,
    destination_path: str
) -> Tuple[str, bool, str]:
    """
    Download a file from Google Drive to a local destination.
    Returns a tuple: (file_name, success_flag, error_message)
    """
    file = None  # Initialize file as None
    try:
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        logger.info(f"Starting download: {destination_path}")

        # Get file metadata to determine the MIME type
        file_metadata = service.files().get(fileId=file_id, fields="mimeType, size, modifiedTime").execute()
        mime_type = file_metadata.get('mimeType')

        if mime_type.startswith('application/vnd.google-apps'):
            # Handle Google Docs, Sheets, Slides, etc.
            export_mime_type, extension = get_export_mime_type_and_extension(mime_type, file_name)
            if not export_mime_type:
                logger.info(f"Skipping unsupported Google file type: {mime_type}")
                return (file_name, False, "Unsupported MIME type")

            request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
            file_name = append_extension(file_name, extension)
            destination_path = os.path.join(os.path.dirname(destination_path), file_name)

            file = io.FileIO(destination_path, 'wb')  # Assign to 'file' before using
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(f"Exporting {file_name}: {int(status.progress() * 100)}% complete")
        else:
            request = service.files().get_media(fileId=file_id)
            file = io.FileIO(destination_path, 'wb')  # Assign to 'file' before using
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Downloading {file_name}: {int(status.progress() * 100)}% complete")
                except ssl.SSLError as e:
                    logger.warning(f"SSL error during download of {file_name}: {e}. Retrying...")
                    raise
                except HttpError as e:
                    if e.resp.status in [403, 429]:  # Rate limit or quota exceeded
                        logger.warning(f"Quota exceeded or rate limit for file {file_name}. Skipping.")
                        return (file_name, False, "Quota or rate limit exceeded")
                    else:
                        logger.error(f"HTTP error during download of {file_name}: {e}")
                        logger.error(traceback.format_exc())
                        raise

        # Save metadata about the downloaded file
        metadata = {
            'file_id': file_id,
            'size': int(file_metadata.get('size', 0)),
            'modified_time': file_metadata.get('modifiedTime')
        }
        with open(destination_path + '.meta', 'w') as meta_file:
            json.dump(metadata, meta_file)
        logger.info(f"Downloaded and saved metadata for {file_name}")

        return (file_name, True, "")

    except ssl.SSLError as e:
        logger.error(f"SSL error while downloading file {file_name}: {e}")
        logger.error(traceback.format_exc())
        return (file_name, False, f"SSL error: {e}")
    except Exception as e:
        logger.error(f"An error occurred while downloading file {file_name}: {e}")
        logger.error(traceback.format_exc())
        return (file_name, False, str(e))
    finally:
        if file:
            try:
                file.close()
                logger.debug(f"Closed file: {destination_path}")
            except Exception as close_error:
                logger.error(f"Error closing file {destination_path}: {close_error}")
                logger.error(traceback.format_exc())

def get_export_mime_type_and_extension(mime_type: str, file_name: str) -> Tuple[str, str]:
    """
    Determine the export MIME type and corresponding file extension based on Google Docs MIME type.
    """
    export_mime_types = {
        'application/vnd.google-apps.document': ('application/pdf', '.pdf'),
        'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
        'application/vnd.google-apps.presentation': ('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx')
    }
    return export_mime_types.get(mime_type, (None, ''))

def append_extension(file_name: str, extension: str) -> str:
    """
    Append the appropriate file extension based on MIME type.
    """
    return file_name if file_name.endswith(extension) else f"{file_name}{extension}"

@retry(
    retry=retry_if_exception_type((ConnectionResetError, OSError, ssl.SSLError, HttpError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def file_already_exists(
    destination_path: str,
    file_id: str,
    service: Any
) -> bool:
    """
    Check if a file already exists and matches the version on Google Drive by comparing metadata.
    """
    metadata_file_path = destination_path + '.meta'
    if not os.path.exists(destination_path) or not os.path.exists(metadata_file_path):
        return False

    try:
        with open(metadata_file_path, 'r') as meta_file:
            metadata = json.load(meta_file)
    except (json.JSONDecodeError, IOError):
        logger.warning(f"Corrupted metadata file found: {metadata_file_path}. Deleting and re-downloading.")
        os.remove(metadata_file_path)
        return False

    try:
        drive_metadata = service.files().get(fileId=file_id, fields="size, modifiedTime").execute()
        drive_file_size = int(drive_metadata.get('size', 0))
        drive_modified_time = drive_metadata.get('modifiedTime')
    except HttpError as e:
        logger.error(f"Error fetching metadata for file ID {file_id}: {e}")
        logger.error(traceback.format_exc())
        return False
    except ssl.SSLError as e:
        logger.error(f"SSL error while fetching metadata for file ID {file_id}: {e}")
        logger.error(traceback.format_exc())
        raise

    if (
        metadata.get('file_id') == file_id and
        metadata.get('size') == drive_file_size and
        metadata.get('modified_time') == drive_modified_time
    ):
        logger.info(f"File already exists and is up-to-date: {destination_path}")
        return True

    return False

def authenticate() -> Any:
    """
    Authenticate and return a Google Drive API service instance.
    """
    creds = None
    try:
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        # Create SSL context with enforced TLS version
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2  # Enforce TLSv1.2 or higher

        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        logger.info("Authentication successful.")
        return service

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        logger.error(traceback.format_exc())
        raise

def get_all_files(service: Any, folder_id: str, parent_path: str = '') -> List[Dict[str, Any]]:
    """
    Recursively retrieve all files (not folders) within the specified folder and its subfolders.
    """
    try:
        items = list_files_in_folder(service, folder_id)
        all_files = []
        for item in items:
            item_name = sanitize_filename(item['name'])
            item_path = os.path.join(parent_path, item_name)
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                # recursively get all files
                subfolder_files = get_all_files(service, item['id'], item_path)
                all_files.extend(subfolder_files)
            else:
                all_files.append({
                    'id': item['id'],
                    'name': item_name,
                    'path': item_path  # Store relative path for each file
                })
        return all_files
    except Exception as e:
        logger.error(f"Error retrieving files from folder ID {folder_id}: {e}")
        logger.error(traceback.format_exc())
        return []

def process_subdirectory(
    service: Any,
    folder_info: Dict[str, Any],
    base_directory: str,
    executor: ThreadPoolExecutor,
    counters: Dict[str, Any],
    lock: threading.Lock
) -> None:
    """
    Process each first-level subdirectory within a shared folder: collect files (including in deeper subfolders),
    download them, and report stats.
    """
    shared_folder_id = folder_info.get('id')
    dest_dir = sanitize_filename(folder_info.get('dest_dir'))
    shared_folder_local_path = os.path.join(base_directory, dest_dir)

    os.makedirs(shared_folder_local_path, exist_ok=True)

    logger.info(f"Starting download assessment for dest '{shared_folder_local_path}' and for folder ID '{shared_folder_id}'.")
    print(f"\nStarting download assessment for dest '{shared_folder_local_path}' and for folder ID '{shared_folder_id}'.")

    # Retrieve all files recursively within the shared folder
    try:
        all_files = get_all_files(service, shared_folder_id)
    except Exception as e:
        logger.error(f"Failed to retrieve files in shared folder {shared_folder_id}: {e}")
        return

    if not all_files:
        logger.info(f"No files found in shared folder '{shared_folder_id}'.")
        print(f"No files found in shared folder '{shared_folder_id}'.")
        return

    # Filter out files that have not been downloaded
    files_to_download = []
    for item in all_files:
        destination_path = os.path.join(shared_folder_local_path, item['path'])  # Use full relative path
        if file_already_exists(destination_path, item['id'], service):
            logger.info(f"Skipping already downloaded file: {destination_path} ({item['id']})")
            with lock:
                counters['skipped'] += 1
            continue
        else:
            files_to_download.append((item['id'], item['name'], destination_path))

    total_files = len(files_to_download)
    with lock:
        counters['total_files'] += total_files

    if total_files == 0:
        logger.info(f"No new files to download in subfolder '{shared_folder_id}'.")
        print(f"No new files to download in subfolder '{shared_folder_id}'.")
        return

    # Initialize progress bar for this subdirectory
    progress_bar = tqdm(total=total_files, desc=f"Downloading '{dest_dir}'", unit="file")

    # Track download statistics for this subdirectory
    sub_counters = {
        'downloaded': 0,
        'skipped': 0,
        'failed': 0
    }

    future_to_file = {}
    for file_id, file_name, destination_path in files_to_download:
        future = executor.submit(download_file, service, file_id, file_name, destination_path)
        future_to_file[future] = file_name

    for future in as_completed(future_to_file):
        file_name = future_to_file[future]
        try:
            result = future.result()
            _, success, error_msg = result
            if success:
                with lock:
                    counters['downloaded'] += 1
                    sub_counters['downloaded'] += 1
            else:
                with lock:
                    counters['failed'] += 1
                    sub_counters['failed'] += 1
                    logger.warning(f"Failed to download {file_name}: {error_msg}")
        except Exception as e:
            with lock:
                counters['failed'] += 1
                sub_counters['failed'] += 1
            logger.error(f"Unhandled exception downloading {file_name}: {e}")
            logger.error(traceback.format_exc())
        finally:
            progress_bar.update(1)

    progress_bar.close()

    # Print and log statistics for this subdirectory
    print(f"\nCompleted downloading '{dest_dir}':")
    print(f"  Successfully Downloaded: {sub_counters['downloaded']}")
    print(f"  Skipped: {sub_counters['skipped']}")
    print(f"  Failed: {sub_counters['failed']}")
    logger.info(f"Completed downloading '{dest_dir}': Downloaded={sub_counters['downloaded']}, Skipped={sub_counters['skipped']}, Failed={sub_counters['failed']}")

def load_config(config_path: str = 'config.json') -> Dict[str, Any]:
    """
    Load and validate configuration from a JSON file.
    """
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)

    # Validate required fields
    if 'gdrive-shared-dir' not in config_data:
        logger.error("'gdrive-shared-dir' not found in configuration.")
        raise KeyError("'gdrive-shared-dir' not found in configuration.")

    return config_data

def move_files(service: Any, folder_id: str, base_directory: str) -> None:
    """
    Move already downloaded files into the correct folder structure.
    """
    logger.info("Starting file organization process without downloading...")
    
    try:
        all_files = get_all_files(service, folder_id)
    except Exception as e:
        logger.error(f"Failed to retrieve files in shared folder {folder_id}: {e}")
        return

    for item in all_files:
        file_name = sanitize_filename(item['name'])
        destination_path = os.path.join(base_directory, file_name)
        correct_folder_path = os.path.join(base_directory, item['path'])  # Get the correct folder path

        if os.path.exists(destination_path):
            # Create the correct folder structure if it doesn't exist
            os.makedirs(os.path.dirname(correct_folder_path), exist_ok=True)

            # Move the file to the correct folder
            try:
                shutil.move(destination_path, correct_folder_path)
                logger.info(f"Moved file '{file_name}' to '{correct_folder_path}'")
            except Exception as e:
                logger.error(f"Failed to move file {file_name} to {correct_folder_path}: {e}")
        else:
            logger.warning(f"File '{file_name}' not found in the destination folder, skipping...")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Google Drive Download and Organization Script")
    parser.add_argument('--move-only', action='store_true', help="Move already downloaded files into correct folders without downloading")
    return parser.parse_args()

def main() -> None:
    base_directory = os.getcwd()

    args = parse_arguments()

    try:
        config_data = load_config()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        print(f"Failed to load configuration: {e}")
        return

    shared_folder_data = config_data.get('gdrive-shared-dir', [])

    try:
        service = authenticate()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        print(f"Authentication failed: {e}")
        return

    if args.move_only:
        # Perform file organization without downloading
        for folder_info in shared_folder_data:
            move_files(service, folder_info['id'], base_directory)
    else:
        num_workers = config_data.get('num_workers', 4)  # Default to 4 workers

        # Initialize global counters
        counters = {
            'skipped': 0,
            'failed': 0,
            'downloaded': 0,
            'total_files': 0
        }

        # Lock for thread-safe operations
        lock = threading.Lock()

        try:
            # Initialize ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                for folder_info in shared_folder_data:
                    process_subdirectory(
                        service,
                        folder_info,
                        base_directory,
                        executor,
                        counters,
                        lock
                    )
        except Exception as e:
            logger.error(f"Unhandled exception in main thread: {e}")
            logger.error(traceback.format_exc())
            print(f"Unhandled exception in main thread: {e}")

        print("\nDownload Summary:")
        print(f"Total Files: {counters['total_files']}")
        print(f"Successfully Downloaded: {counters['downloaded']}")
        print(f"Skipped: {counters['skipped']}")
        print(f"Failed: {counters['failed']}")

        logger.info("Download process completed.")
        logger.info(f"Total Files: {counters['total_files']}")
        logger.info(f"Successfully Downloaded: {counters['downloaded']}")
        logger.info(f"Skipped: {counters['skipped']}")
        logger.info(f"Failed: {counters['failed']}")

if __name__ == '__main__':
    main()
