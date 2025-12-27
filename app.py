import os
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from drive_connector import DriveConnector
from analyzer import Analyzer

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize Bot Components
drive = None
analyzer = None

def init_bot():
    global drive, analyzer
    if drive and analyzer:
        return True
    
    print("--- Initializing Bot Components ---")
    try:
        drive = DriveConnector()
    except Exception as e:
        print(f"Error connecting to Drive: {e}")
        return False

    analyzer = Analyzer()
    if not analyzer.api_key:
        print("Error: OPENAI_API_KEY not set.")
        return False
    
    return True

def load_drive_context():
    if not drive:
        if not init_bot():
            return "Bot initialization failed."
    
    folder_id = os.getenv("DRIVE_FOLDER_ID")
    if not folder_id:
        return "Error: DRIVE_FOLDER_ID not set."

    try:
        print("Fetching files list from Drive...")
        files = drive.list_files(folder_id)
        if not files:
            return "No files found in Drive folder."
        
        # Determine which files need downloading or deleting
        indexed_info = analyzer.vector_store.get_indexed_files_info()
        
        files_data = []
        for f in files:
            name = f['name']
            drive_modified = f.get('modifiedTime')
            db_modified = indexed_info.get(name)
            
            # Download only if new or modified
            if not db_modified or db_modified != drive_modified:
                print(f"Downloading new/modified file: {name}")
                content = drive.download_file_content(f['id'])
                if content:
                    files_data.append({
                        'id': f['id'],
                        'name': name,
                        'content': content,
                        'mimeType': f['mimeType'],
                        'modifiedTime': drive_modified
                    })
            else:
                # Add to files_data but without content (analyzer will know to skip)
                # Actually, analyzer needs current_files set to know what to DELETE.
                # So we pass the metadata at least.
                files_data.append({
                    'name': name,
                    'modifiedTime': drive_modified
                    # No content means it won't be re-indexed
                })
        
        print(f"Syncing with Vector DB (processing {sum(1 for f in files_data if 'content' in f)} changes)...")
        result = analyzer.process_and_index_files(files_data)
        return result
    except Exception as e:
        return f"Error loading context: {e}"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/refresh', methods=['POST'])
def refresh_context():
    msg = load_drive_context()
    return jsonify({"message": msg})

@app.route('/api/ask', methods=['POST'])
def ask():
    init_bot() # Ensure components are ready
    
    data = request.json
    query = data.get('query')
    
    if not query:
        return jsonify({"error": "No query provided"}), 400
        
    result = analyzer.ask_bot(query)
    return jsonify(result)  # Now returns {"answer": "...", "sources": [...]}

# Pre-initialize components and tables
init_bot()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
