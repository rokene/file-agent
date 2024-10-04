import os
import io
import pickle
import mimetypes
import re
import json
import logging
import time
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from logging.handlers import RotatingFileHandler

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

def file_already_exists(destination_folder, file_name, file_id, service):
    """Check if a file already exists and matches the version on Google Drive by comparing file ID, size, and modified time."""
    file_path = os.path.join(destination_folder, file_name)
    if not os.path.exists(file_path):
        return False

    # Get file metadata from Google Drive to compare
    try:
        file_metadata = service.files().get(fileId=file_id, fields="size, modifiedTime").execute()
        drive_file_size = int(file_metadata.get('size', 0))
        drive_modified_time = file_metadata.get('modifiedTime')
    except Exception as e:
        logger.error(f"Error fetching metadata for file ID {file_id}: {str(e)}")
        return False

    # Check if metadata file exists for local file
    metadata_file_path = file_path + '.meta'
    if not os.path.exists(metadata_file_path):
        return False

    # Read the stored metadata with error handling for corrupted files
    try:
        with open(metadata_file_path, 'r') as meta_file:
            metadata = json.load(meta_file)
    except json.JSONDecodeError:
        logger.warning(f"Corrupted metadata file found: {metadata_file_path}. Deleting and re-downloading.")
        os.remove(metadata_file_path)
        return False

    existing_file_id = metadata.get('file_id')
    existing_file_size = metadata.get('size')
    existing_modified_time = metadata.get('modified_time')

    # Compare Google Drive file ID, size, and modified time with local metadata
    if (existing_file_id == file_id and
        existing_file_size == drive_file_size and
        existing_modified_time == drive_modified_time):
        return True

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

def list_files_in_folder(service, folder_id):
    """List all files in a specified Google Drive folder."""
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()
    items = results.get('files', [])
    return items

def download_file(service, file_id, file_name, destination_folder, counters):
    """Download a file from Google Drive to a local destination."""
    try:
        file_path = os.path.join(destination_folder, file_name)
        logger.info(f"Downloading to: {file_path}")  # Log the full file path

        # Get file metadata to determine the MIME type
        file_metadata = service.files().get(fileId=file_id, fields="mimeType, size, modifiedTime").execute()
        mime_type = file_metadata.get('mimeType')
        drive_file_size = int(file_metadata.get('size', 0))
        drive_modified_time = file_metadata.get('modifiedTime')

        if mime_type.startswith('application/vnd.google-apps'):
            # Handle Google Docs, Sheets, Slides, etc.
            if mime_type == 'application/vnd.google-apps.document':
                export_mime_type = 'application/pdf'  # You can change this to another export type if needed
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
                file_name += '.pdf'  # Add the correct extension if exporting to PDF

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
                        # Catch the download quota exceeded or rate limit exceeded error
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

def download_all_files_in_folder(service, folder_id, destination_root, counters):
    """Download all files in a specified Google Drive folder under a given destination root."""
    # List all items in the current folder
    files = list_files_in_folder(service, folder_id)

    for file in files:
        file_id = file['id']
        file_name = sanitize_filename(file['name'])

        # Get the file metadata to determine if it is a folder
        file_metadata = service.files().get(fileId=file_id, fields="mimeType").execute()
        mime_type = file_metadata.get('mimeType')

        if mime_type == 'application/vnd.google-apps.folder':
            # If the item is a folder, recursively download its contents
            logger.info(f"Found folder: {file_name} ({file_id})")
            new_destination = os.path.join(destination_root, file_name)
            if not os.path.exists(new_destination):
                os.makedirs(new_destination)
            download_all_files_in_folder(service, file_id, new_destination, counters)
        else:
            # If the item is a file, download it if it does not already exist
            if not file_already_exists(destination_root, file_name, file_id, service):
                logger.info(f"Downloading file: {file_name} ({file_id})")
                download_file(service, file_id, file_name, destination_root, counters)
            else:
                logger.info(f"Skipping existing file: {file_name}")
                counters['skipped'] += 1
                update_terminal_status(counters)

def update_terminal_status(counters):
    """Update terminal status line with current counts of downloaded, skipped, and failed files."""
    print(f"\rSuccessfully Downloaded: {counters['downloaded']} | Skipped: {counters['skipped']} | Failed: {counters['failed']}", end='', flush=True)

if __name__ == '__main__':
    base_directory = os.getcwd()  # Use the current working directory
    shared_folder_data = None

    with open('config.json', 'r') as config_file:
        shared_folder_data = json.load(config_file)

    service = authenticate()

    # Initialize counters for skipped, failed, and downloaded items
    counters = {
        'skipped': 0,
        'failed': 0,
        'downloaded': 0,
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
        download_all_files_in_folder(service, folder_id, root_destination, counters)

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
