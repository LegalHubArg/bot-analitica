import os
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class DriveConnector:
    def __init__(self):
        self.creds = None
        self.service = None
        self._authenticate()

    def _authenticate(self):
        """Authenticates using Service Account."""
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not creds_path or not os.path.exists(creds_path):
            raise FileNotFoundError(f"Credentials file not found at: {creds_path}")
        
        try:
            self.creds = service_account.Credentials.from_service_account_file(
                creds_path, scopes=SCOPES)
            self.service = build('drive', 'v3', credentials=self.creds)
            print("Successfully authenticated with Google Drive.")
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise

    def list_files(self, folder_id):
        """Lists files in the specified folder."""
        if not self.service:
            print("Drive service not initialized.")
            return []

        try:
            results = self.service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                pageSize=100,
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)").execute()
            items = results.get('files', [])
            return items
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []

    def download_file_content(self, file_id):
        """Downloads file content as a string (if text/csv)."""
        if not self.service:
            return None

        try:
            request = self.service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            return file.getvalue()
        except HttpError as error:
            print(f"An error occurred downloading file {file_id}: {error}")
            return None
