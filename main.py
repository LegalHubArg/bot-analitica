import os
import sys
from dotenv import load_dotenv
from drive_connector import DriveConnector
from analyzer import Analyzer

# Load env vars
load_dotenv()

def main():
    print("--- Google Drive Analytics Bot ---")
    
    # 1. Setup Drive
    try:
        drive = DriveConnector()
    except Exception as e:
        print(f"Critical Error: Could not connect to Drive. {e}")
        return

    folder_id = os.getenv("DRIVE_FOLDER_ID")
    if not folder_id:
        print("Error: DRIVE_FOLDER_ID not set in .env")
        return

    # 2. Setup Analyzer
    analyzer = Analyzer()
    if not analyzer.api_key:
        print("Critical Error: OPENAI_API_KEY not set.")
        return

    print("Fetching files from Drive...")
    files = drive.list_files(folder_id)
    if not files:
        print("No files found or error listing files.")
        return

    print(f"Found {len(files)} files.")
    
    # 3. Download and Prepare Context
    # Note: determining what to download. For now, download all found (be careful with large folders)
    # Filter for interesting files?
    files_data = []
    for f in files:
        print(f"Downloading {f['name']}...")
        content = drive.download_file_content(f['id'])
        if content:
            files_data.append({
                'name': f['name'],
                'content': content,
                'mimeType': f['mimeType']
            })
    
    print("Preparing context...")
    context = analyzer.prepare_context(files_data)
    print("Ready! Ask a question (or type 'exit' to quit).")

    # 4. Interactive Loop
    while True:
        user_input = input("\n> ")
        if user_input.lower() in ['exit', 'quit']:
            break
        
        answer = analyzer.ask_bot(user_input, context)
        print(f"\nBot: {answer}")

if __name__ == "__main__":
    main()
