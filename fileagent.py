import os
import io
import pickle
import mimetypes
import re
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Define the scopes for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

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
        print(f"Error fetching metadata for file ID {file_id}: {str(e)}")
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
        print(f"Corrupted metadata file found: {metadata_file_path}. Deleting and re-downloading.")
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

def list_files_in_folder(service, folder_id):
    """List all files in a specified Google Drive folder."""
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()
    items = results.get('files', [])
    return items

def download_file(service, file_id, file_name, destination_folder):
    """Download a file from Google Drive to a local destination."""
    try:
        file_path = os.path.join(destination_folder, file_name)
        print(f"Downloading to: {file_path}")  # Output the full file path

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
                print(f"Skipping unsupported Google file type: {mime_type}")
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
                    print(f"Exporting {file_name}: {int(status.progress() * 100)}% complete")
        else:
            request = service.files().get_media(fileId=file_id)

            with io.FileIO(file_path, 'wb') as file:
                downloader = MediaIoBaseDownload(file, request)
                done = False
                while not done:
                    try:
                        status, done = downloader.next_chunk()
                        print(f"Downloading {file_name}: {int(status.progress() * 100)}% complete")
                    except Exception as e:
                        # Catch the download quota exceeded error
                        if "downloadQuotaExceeded" in str(e):
                            print(f"Download quota exceeded for file: {file_name}. Skipping.")
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

    except Exception as e:
        print(f"An error occurred while downloading file {file_name}: {str(e)}")

def download_all_files_in_folder(service, folder_id, destination_root):
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
            print(f"Found folder: {file_name} ({file_id})")
            new_destination = os.path.join(destination_root, file_name)
            if not os.path.exists(new_destination):
                os.makedirs(new_destination)
            download_all_files_in_folder(service, file_id, new_destination)
        else:
            # If the item is a file, download it if it does not already exist
            if not file_already_exists(destination_root, file_name, file_id, service):
                print(f"Downloading file: {file_name} ({file_id})")
                download_file(service, file_id, file_name, destination_root)
            else:
                print(f"Skipping existing file: {file_name}")

if __name__ == '__main__':
    base_directory = os.getcwd()  # Use the current working directory
    shared_folder_data = None

    with open('config.json', 'r') as config_file:
        shared_folder_data = json.load(config_file)

    service = authenticate()

    for folder_info in shared_folder_data:
        folder_id = folder_info['id']
        dest_name = folder_info['dest_name']

        # Set root destination to keep the folder structure intact under the base directory
        root_destination = os.path.join(base_directory, dest_name)
        if not os.path.exists(root_destination):
            os.makedirs(root_destination)

        print(f"Downloading into {root_destination} from id {folder_id}")
        download_all_files_in_folder(service, folder_id, root_destination)
