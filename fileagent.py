import os
import io
import pickle
import re
import json
import logging
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from logging.handlers import RotatingFileHandler
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from googleapiclient.errors import HttpError

# Define the scopes for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Set up rotating logging
handler = RotatingFileHandler('file_download.log', maxBytes=5*1024*1024, backupCount=5)
logging.basicConfig(
    handlers=[handler],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

def sanitize_filename(filename, max_length=50):
    """Sanitize the filename to remove invalid characters and shorten the length."""
    sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + '...'
    return sanitized

@retry(retry=retry_if_exception_type((ConnectionResetError, OSError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       before_sleep=before_sleep_log(logger, logging.WARNING))
def list_files_in_folder(service, folder_id):
    """List all files in a specified Google Drive folder with retry on errors."""
    try:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])
        return items
    except HttpError as e:
        logger.error(f"Error listing files in folder {folder_id}: {e}")
        raise

@retry(retry=retry_if_exception_type((ConnectionResetError, OSError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       before_sleep=before_sleep_log(logger, logging.WARNING))
def download_file(service, file_id, file_name, destination_folder, counters):
    """Download a file from Google Drive to a local destination with retry on errors."""
    try:
        file_path = os.path.join(destination_folder, file_name)
        logger.info(f"Downloading to: {file_path}")

        # Get file metadata to determine the MIME type
        file_metadata = service.files().get(fileId=file_id, fields="mimeType, size, modifiedTime").execute()
        mime_type = file_metadata.get('mimeType')
        drive_file_size = int(file_metadata.get('size', 0))
        drive_modified_time = file_metadata.get('modifiedTime')

        if mime_type.startswith('application/vnd.google-apps'):
            # Handle Google Docs, Sheets, Slides, etc.
            if mime_type == 'application/vnd.google-apps.document':
                export_mime_type = 'application/pdf'
            elif mime_type == 'application/vnd.google-apps.spreadsheet':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            elif mime_type == 'application/vnd.google-apps.presentation':
                export_mime_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
            else:
                logger.info(f"Skipping unsupported Google file type: {mime_type}")
                counters['skipped'] += 1
                update_terminal_status(counters)
                return

            request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
            if not file_name.endswith('.pdf') and export_mime_type == 'application/pdf':
                file_name += '.pdf'

            file_path = os.path.join(destination_folder, file_name)

            with io.FileIO(file_path, 'wb') as file:
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    logger.info(f"Exporting {file_name}: {int(status.progress() * 100)}% complete")
        else:
            request = service.files().get_media(fileId=file_id)

            with io.FileIO(file_path, 'wb') as file:
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while not done:
                    try:
                        status, done = downloader.next_chunk()
                        logger.info(f"Downloading {file_name}: {int(status.progress() * 100)}% complete")
                    except Exception as e:
                        if "downloadQuotaExceeded" in str(e) or "rateLimitExceeded" in str(e):
                            logger.warning(f"Download quota exceeded or rate limit exceeded for file: {file_name}. Skipping.")
                            counters['skipped'] += 1
                            update_terminal_status(counters)
                            return
                        else:
                            raise

        # Save metadata about the downloaded file (file ID, size, and modified time) to avoid redownloading
        metadata = {
            'file_id': file_id,
            'size': drive_file_size,
            'modified_time': drive_modified_time
        }
        with open(file_path + '.meta', 'w') as meta_file:
            json.dump(metadata, meta_file)

        # Increment downloaded files counter and record file path
        counters['downloaded'] += 1
        counters['downloaded_files'].append(file_path)

    except Exception as e:
        logger.error(f"An error occurred while downloading file {file_name}: {str(e)}")
        counters['failed'] += 1

    # Update terminal output with current status counts
    update_terminal_status(counters)

@retry(retry=retry_if_exception_type((ConnectionResetError, OSError, ssl.SSLError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       before_sleep=before_sleep_log(logger, logging.WARNING))
def file_already_exists(destination_folder, file_name, file_id, service):
    """Check if a file already exists and matches the version on Google Drive by comparing file ID, size, and modified time."""
    file_path = os.path.join(destination_folder, file_name)
    metadata_file_path = file_path + '.meta'

    # Check if both the file and its metadata exist
    if not os.path.exists(file_path) or not os.path.exists(metadata_file_path):
        return False

    # Attempt to read the stored metadata
    try:
        with open(metadata_file_path, 'r') as meta_file:
            metadata = json.load(meta_file)
    except (json.JSONDecodeError, IOError):
        logger.warning(f"Corrupted metadata file found: {metadata_file_path}. Deleting and re-downloading.")
        os.remove(metadata_file_path)  # Delete corrupted metadata to force redownload
        return False

    # Get the file metadata from Google Drive for comparison
    try:
        drive_metadata = service.files().get(fileId=file_id, fields="size, modifiedTime").execute()
        drive_file_size = int(drive_metadata.get('size', 0))
        drive_modified_time = drive_metadata.get('modifiedTime')
    except HttpError as e:
        logger.error(f"Error fetching metadata for file ID {file_id}: {str(e)}")
        return False
    except ssl.SSLError as e:
        logger.error(f"SSL error while fetching metadata for file ID {file_id}: {str(e)}")
        raise  # Trigger retry mechanism

    # Extract information from the stored metadata
    existing_file_id = metadata.get('file_id')
    existing_file_size = metadata.get('size')
    existing_modified_time = metadata.get('modified_time')

    # Compare file ID, size, and modified time to determine if re-download is needed
    if (existing_file_id == file_id and
        existing_file_size == drive_file_size and
        existing_modified_time == drive_modified_time):
        return True

    # If metadata does not match, the file needs to be downloaded again
    return False

def authenticate():
    """Authenticate and return a Google Drive API service instance."""
    creds = None
    try:
        # Token file to store user's credentials
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If no (valid) credentials are available, prompt the user to log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Save the credentials for future use
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        # Build the service instance
        return build('drive', 'v3', credentials=creds)

    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

def download_all_files_in_folder(service, folder_id, destination_root, counters, num_workers):
    """Download all files in a specified Google Drive folder under a given destination root using a thread pool for parallel downloads."""
    # List all items in the current folder
    files = list_files_in_folder(service, folder_id)
    
    # Add to total files count
    counters['total_files'] += len(files)

    # Create a thread pool for downloading files in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {
            executor.submit(download_file, service, file['id'], sanitize_filename(file['name']), destination_root, counters): file
            for file in files
            if file['mimeType'] != 'application/vnd.google-apps.folder' and not file_already_exists(destination_root, sanitize_filename(file['name']), file['id'], service)
        }

        # Process skipped files
        for file in files:
            if file['mimeType'] != 'application/vnd.google-apps.folder' and file_already_exists(destination_root, sanitize_filename(file['name']), file['id'], service):
                counters['skipped'] += 1
                update_terminal_status(counters)

        # Process folders in a non-parallel way to maintain folder structure
        for file in files:
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                logger.info(f"Found folder: {file['name']} ({file['id']})")
                new_destination = os.path.join(destination_root, sanitize_filename(file['name']))
                if not os.path.exists(new_destination):
                    os.makedirs(new_destination)
                download_all_files_in_folder(service, file['id'], new_destination, counters, num_workers)

        # Wait for all parallel tasks to complete
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error downloading file {file['name']} ({file['id']}): {str(e)}")
                counters['failed'] += 1
                update_terminal_status(counters)

def update_terminal_status(counters):
    """Update terminal status line with current counts of downloaded, skipped, and failed files."""
    print(f"\rTotal Files: {counters['total_files']} | Successfully Downloaded: {counters['downloaded']} | Skipped: {counters['skipped']} | Failed: {counters['failed']}", end='', flush=True)

if __name__ == '__main__':
    base_directory = os.getcwd()  # Use the current working directory

    # Load configuration from 'config.json'
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    # Extract number of workers and shared directories list
    num_workers = config_data.get('num_workers', 1)
    shared_folder_data = config_data.get('gdrive-shared-dir', [])

    service = authenticate()

    # Initialize counters for skipped, failed, downloaded items, and total files
    counters = {
        'skipped': 0,
        'failed': 0,
        'downloaded': 0,
        'total_files': 0,
        'downloaded_files': []  # List to keep track of downloaded files
    }

    for folder_info in shared_folder_data:
        folder_id = folder_info['id']
        dest_dir = folder_info['dest_dir']

        # Set root destination to keep the folder structure intact under the base directory
        root_destination = os.path.join(base_directory, dest_dir)
        if not os.path.exists(root_destination):
            os.makedirs(root_destination)

        logger.info(f"Downloading into {root_destination} from id {folder_id}")
        update_terminal_status(counters)
        download_all_files_in_folder(service, folder_id, root_destination, counters, num_workers)

    # Print summary of downloaded, skipped, and failed items
    print()  # Move to a new line after the last status update
    logger.info("\nSummary:")
    logger.info(f"Downloaded files: {counters['downloaded']}")
    logger.info(f"Skipped files: {counters['skipped']}")
    logger.info(f"Failed files: {counters['failed']}")

    # Print list of downloaded files
    if counters['downloaded'] > 0:
        logger.info("\nList of downloaded files:")
        for downloaded_file in counters['downloaded_files']:
            logger.info(downloaded_file)
