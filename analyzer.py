import os
import io
import pandas as pd
import requests
import json
from openai import OpenAI
from dotenv import load_dotenv
from vector_store import VectorStore
from PyPDF2 import PdfReader

load_dotenv()

class Analyzer:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            print("Warning: OPENAI_API_KEY not found in .env")
        self.client = OpenAI(api_key=self.api_key)
        
        # Initialize Vector Store if DB_URL is present, else None
        try:
            print("Attempting to initialize Vector Store...")
            self.vector_store = VectorStore()
            print("VectorStore created, initializing database...")
            self.vector_store.init_db()
            print("Vector Store initialized successfully!")
        except Exception as e:
            print(f"ERROR: Vector Store initialization failed!")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            import traceback
            traceback.print_exc()
            self.vector_store = None

    def get_embedding(self, text):
        """Generates embedding for a given text."""
        text = text.replace("\n", " ")
        return self.client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

    def sanitize_text(self, text):
        """Removes NUL characters and other problematic characters from text."""
        if not text:
            return ""
        # Remove NUL bytes and other control characters except newlines and tabs
        return ''.join(char for char in text if char == '\n' or char == '\t' or ord(char) >= 32)

    def chunk_text(self, text, chunk_size=2000, overlap=200):
        """Splits text into chunks."""
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start = end - overlap
        return chunks

    def get_weather(self, location):
        """Fetches current weather using wttr.in."""
        try:
            # wttr.in provides a simple text or JSON interface
            # format=3 gives back something like "Oliveros: ⛅️ +25°C"
            url = f"https://wttr.in/{location}?format=j1"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                current = data['current_condition'][0]
                temp = current['temp_C']
                desc = current['lang_es'][0]['value'] if 'lang_es' in current else current['weatherDesc'][0]['value']
                return f"El clima en {location} es {desc} con una temperatura de {temp}°C."
            return f"No pude obtener el clima para {location}."
        except Exception as e:
            return f"Error consultando el clima: {e}"

    def process_and_index_files(self, files_data):
        """
        Processes files, generates embeddings, and indexes them in the Vector Database.
        Only processes new files and removes deleted ones (incremental update).
        """
        if not self.vector_store:
            return "Error: Database not connected."

        # Get currently indexed files info {filename: modified_at}
        indexed_files_info = self.vector_store.get_indexed_files_info()
        
        # Files currently in Drive
        drive_files_map = {f['name']: f for f in files_data if f.get('name')}
        
        # 1. Find files to delete (in DB but not in Drive)
        files_to_delete = set(indexed_files_info.keys()) - set(drive_files_map.keys())
        for filename in files_to_delete:
            print(f"Removing deleted file: {filename}")
            self.vector_store.delete_by_filename(filename)
        
        # 2. Find files to process (New or Modified)
        files_to_process_names = []
        for name, drive_file in drive_files_map.items():
            db_modified_at = indexed_files_info.get(name)
            drive_modified_at = drive_file.get('modifiedTime')
            
            if not db_modified_at or db_modified_at != drive_modified_at:
                files_to_process_names.append(name)
                # If it's modified, delete old version first
                if db_modified_at:
                    print(f"File modified, updating: {name}")
                    self.vector_store.delete_by_filename(name)
        
        if not files_to_process_names and not files_to_delete:
            return f"No changes detected. {len(indexed_files_info)} files already indexed."
        
        print(f"Processing {len(files_to_process_names)} new/modified files, removed {len(files_to_delete)} files...")

        documents_to_add = []

        print(f"Processing {len(files_data)} files...")
        for name in files_to_process_names:
            file = drive_files_map[name]
            # name = file.get('name') # already have name
            content = file.get('content')
            mime = file.get('mimeType')
            modified_at = file.get('modifiedTime')
            
            if not content:
                continue

            text_content = ""
            try:
                if 'csv' in mime or name.endswith('.csv'):
                    try:
                        df = pd.read_csv(io.BytesIO(content))
                        # For CSVs, we treat the summary as one rich chunk
                        text_content = f"CSV File: {name}\nColumns: {list(df.columns)}\nInfo:\n{df.info(buf=io.StringIO())}\n\nSample Data (First 50 rows):\n{df.head(50).to_csv(index=False)}"
                    except Exception as e:
                        print(f"Error reading CSV {name}: {e}")
                        continue
                elif 'sheet' in mime or name.endswith('.xlsx') or name.endswith('.xls'):
                    try:
                        df = pd.read_excel(io.BytesIO(content))
                        text_content = f"Excel File: {name}\nColumns: {list(df.columns)}\nInfo:\n{df.info(buf=io.StringIO())}\n\nSample Data (First 50 rows):\n{df.head(50).to_csv(index=False)}"
                    except Exception as e:
                        print(f"Error reading Excel {name}: {e}")
                        continue
                elif 'pdf' in mime or name.endswith('.pdf'):
                    try:
                        pdf_reader = PdfReader(io.BytesIO(content))
                        pdf_text_parts = []
                        for page_num, page in enumerate(pdf_reader.pages):
                            page_text = page.extract_text()
                            if page_text:
                                pdf_text_parts.append(f"Page {page_num + 1}:\n{page_text}")
                        text_content = f"PDF File: {name}\n\n" + "\n\n".join(pdf_text_parts)
                    except Exception as e:
                        print(f"Error reading PDF {name}: {e}")
                        continue
                elif 'text' in mime or name.endswith('.txt') or name.endswith('.md') or mime == 'application/vnd.google-apps.document':
                     # Note: Google Docs might need implicit conversion if exportFormat wasn't handled in drive_connector. 
                     # Assuming drive_connector downloads as text/plain or relevant format.
                    try:
                        text_content = content.decode('utf-8', errors='ignore')
                    except AttributeError:
                        text_content = str(content)
                else:
                    # Generic text attempt
                    try:
                         text_content = content.decode('utf-8', errors='ignore')
                    except:
                         print(f"Skipping unsupported file: {name}")
                         continue

                # Sanitize text to remove NUL characters
                text_content = self.sanitize_text(text_content)
                
                # Batch chunks
                chunks = self.chunk_text(text_content)
                for chunk in chunks:
                    if not chunk.strip(): 
                        continue
                    
                    # Sanitize chunk as well
                    chunk = self.sanitize_text(chunk)
                    
                    # Generate embedding
                    embedding = self.get_embedding(chunk)
                    
                    documents_to_add.append({
                        'embedding_text': f"Vino/Documento: {name}\nContenido: {chunk}",
                        'metadata': {
                            'identificacion': {
                                'nombre': name,
                                'vino_id': name.split('.')[0]
                            },
                            'documental': {
                                'fuente_nombre': name, 
                                'fecha_ingesta': modified_at,
                                'tipo_chunk': 'fragmento_texto'
                            }
                        },
                        'embedding': embedding
                    })

            except Exception as e:
                print(f"Error processing {name}: {e}")

        # Bulk insert
        if documents_to_add:
            print(f"Indexing {len(documents_to_add)} chunks...")
            self.vector_store.add_documents(documents_to_add)
            return f"Successfully indexed {len(documents_to_add)} chunks from {len(files_data)} files."
        else:
            return "No content found to index."

    def ask_bot(self, query, context=None): 
        """
        Sends query to OpenAI using RAG and Tools (Weather Agent).
        """
        retrieved_context = ""
        source_files = set()
        
        if self.vector_store:
            try:
                query_embedding = self.get_embedding(query)
                results = self.vector_store.search(query_embedding, limit=5)
                context_parts = []
                for res in results:
                    context_parts.append(f"--- Ficha Técnica / Fragmento ---\n{res['embedding_text']}")
                    if res.get('metadata') and res['metadata'].get('documental'):
                        source_files.add(res['metadata']['documental'].get('fuente_nombre'))
                retrieved_context = "\n\n".join(context_parts)
            except Exception as e:
                print(f"RAG Search failed: {e}")

        system_prompt = (
            "You are a helpful data analytics assistant. "
            "Use the provided context to answer questions about documents. "
            "If you need to know the weather, use the 'get_weather' tool. "
            "For Oliveros, Santa Fe, always use the 'get_weather' tool when asked. "
            "Do NOT mention source files in your response."
        )

        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get the current weather for a specific location",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {
                                "type": "string",
                                "description": "The city and state, e.g. Oliveros, Santa Fe, Argentina",
                            }
                        },
                        "required": ["location"],
                    },
                },
            }
        ]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Context:\n{retrieved_context}\n\nQuestion: {query}"}
        ]

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0.5
            )
            
            response_message = response.choices[0].message
            tool_calls = response_message.tool_calls

            if tool_calls:
                messages.append(response_message)
                used_weather = False
                for tool_call in tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)
                    
                    if function_name == "get_weather":
                        used_weather = True
                        function_response = self.get_weather(location=function_args.get("location"))
                        messages.append({
                            "tool_call_id": tool_call.id,
                            "role": "tool",
                            "name": function_name,
                            "content": function_response,
                        })
                
                # Get a new response after tool execution
                second_response = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                )
                answer = second_response.choices[0].message.content
                
                # Do not show document sources if the weather agent was used
                if used_weather:
                    source_files = set()
            else:
                answer = response_message.content
            
            return {
                "answer": answer,
                "sources": list(source_files) if source_files else []
            }
        except Exception as e:
            return {
                "answer": f"Error communicating with OpenAI: {e}",
                "sources": []
            }
