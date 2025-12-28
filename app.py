import os
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from drive_connector import DriveConnector
from analyzer import Analyzer

# Load environment variables
load_dotenv()

APP_VERSION = "1.3.0-intelligent-extraction"

app = Flask(__name__)

# Initialize Bot Components
drive = None
analyzer = None

def init_bot():
    global drive, analyzer
    
    print("--- Initializing Bot Components (Resilient Mode) ---")
    
    if not drive:
        try:
            drive = DriveConnector()
        except Exception as e:
            print(f"Drive initialization FAILED: {e}")

    if not analyzer:
        analyzer = Analyzer()
        if not analyzer.api_key:
            print("Warning: OPENAI_API_KEY not set.")
    
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
    return render_template('index.html', version=APP_VERSION)

@app.route('/api/refresh', methods=['POST'])
def refresh_context():
    data = request.json or {}
    msg_prefix = ""
    if data.get('force') is True:
        if analyzer and analyzer.vector_store:
            num = analyzer.vector_store.clear_all_chunks()
            msg_prefix = f"Forced refresh: {num} chunks cleared. "
    
    msg = load_drive_context()
    return jsonify({"message": msg_prefix + msg})

@app.route('/api/ask', methods=['POST'])
def ask():
    init_bot() # Ensure components are ready
    
    data = request.json
    query = data.get('query')
    
    if not query:
        return jsonify({"error": "No query provided"}), 400
        
    result = analyzer.ask_bot(query)
    return jsonify(result)  # Now returns {"answer": "...", "sources": [...]}

@app.route('/api/wines')
def get_wines():
    if not analyzer or not analyzer.vector_store:
        return jsonify({"error": "Database not initialized"}), 500
    try:
        wines = analyzer.vector_store.get_unique_labels()
        return jsonify(wines)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/debug/db')
def debug_db():
    init_bot() # Force init attempt
    
    debug_info = {
        "status": "pending",
        "version": APP_VERSION,
        "drive_initialized": drive is not None,
        "analyzer_initialized": analyzer is not None,
    }

    if not analyzer:
        debug_info["status"] = "error"
        debug_info["message"] = "Analyzer not initialized (check OpenAI key)"
        return jsonify(debug_info)

    if not analyzer.vector_store:
        debug_info["status"] = "error"
        debug_info["message"] = "VectorStore not initialized (likely DB connection or init_db failed)"
        debug_info["init_error"] = getattr(analyzer, 'init_error', 'No error captured')
        return jsonify(debug_info)
    
    try:
        from sqlalchemy import inspect
        session = analyzer.vector_store.Session()
        inspector = inspect(analyzer.vector_store.engine)
        tables = inspector.get_table_names()
        
        db_url_masked = os.getenv("DATABASE_URL", "NOT SET")
        if "@" in db_url_masked:
             prefix, rest = db_url_masked.split("@", 1)
             db_url_masked = f"{prefix.split(':')[0]}@***{rest}"

        # Get a sample record to check JSON format
        from vector_store import WineChunk
        sample_record = session.query(WineChunk).first()
        sample_data = None
        if sample_record:
            sample_data = {
                "id": sample_record.id,
                "embedding_text_preview": sample_record.embedding_text[:100] + "..." if sample_record.embedding_text else None,
                "meta_data": sample_record.meta_data
            }

        debug_info.update({
            "status": "ok",
            "tables": tables,
            "database_url_masked": db_url_masked,
            "engine_dialect": str(analyzer.vector_store.engine.dialect.name),
            "sample_record": sample_data
        })
        return jsonify(debug_info)
    except Exception as e:
        debug_info.update({"status": "error", "message": str(e)})
        return jsonify(debug_info)
    finally:
        if 'session' in locals() and session:
            session.close()

# Pre-initialize components and tables safely during boot
try:
    print(f"--- STARTING APP VERSION {APP_VERSION} ---")
    init_bot()
except Exception as e:
    print(f"--- WARNING: Pre-initialization failed during boot. Error: {e} ---")
    print("--- The app will attempt to initialize again on the first request. ---")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
