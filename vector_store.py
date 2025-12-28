import os
import json
from sqlalchemy import create_engine, Column, Integer, String, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class WineChunk(Base):
    __tablename__ = 'wine_chunks'

    id = Column(Integer, primary_key=True)
    embedding_text = Column(Text)  # Texto especializado para vectorizar
    meta_data = Column(JSONB)       # Metadata estructurada (identificación, origen, enología, etc.)
    embedding = Column(Vector(1536))

class VectorStore:
    def __init__(self):
        # Prefer individual env vars for safer special char handling, fallback to DATABASE_URL
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS")
        db_name = os.getenv("DB_NAME")
        db_host = os.getenv("DB_HOST")
        
        if db_user and db_pass and db_name and db_host:
            from urllib.parse import quote_plus
            # Encode password to handle special chars like '!'
            encoded_pass = quote_plus(db_pass)
            db_url = f"postgresql+psycopg2://{db_user}:{encoded_pass}@/{db_name}?host={db_host}"
            print(f"Using constructed DATABASE_URL with user '{db_user}' and host '{db_host}'")
        else:
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                raise ValueError("DATABASE_URL or (DB_USER, DB_PASS, DB_NAME, DB_HOST) must be set")
            print("Using provided DATABASE_URL environment variable.")

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def init_db(self):
        """Creates the vector extension and tables if they don't exist."""
        print(f"Connecting to database to initialize tables... (Dialect: {self.engine.dialect.name})")
        try:
            with self.engine.connect() as conn:
                print("Enabling pgvector extension if not exists...")
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                conn.commit()
                print("Extension enabled.")
            
            print(f"Creating tables for models defined in Base (including '{WineChunk.__tablename__}')...")
            Base.metadata.create_all(self.engine)
            print("Database initialized (extension + specialized wine_chunks table).")
        except Exception as e:
            print(f"CRITICAL ERROR in init_db: {str(e)}")
            raise e

    def clear_documents(self):
        """Clears all wine chunks from the store."""
        with self.Session() as session:
            session.query(WineChunk).delete()
            session.commit()
        print("All records cleared.")
    
    def get_indexed_files_info(self):
        """Returns a dictionary of {filename: modified_time} for all indexed files."""
        with self.Session() as session:
            results = session.query(
                WineChunk.meta_data['documental']['fuente_nombre'].astext,
                WineChunk.meta_data['documental']['fecha_ingesta'].astext
            ).distinct().all()
            return {r[0]: r[1] for r in results if r[0]}
    
    def delete_by_filename(self, filename):
        """Deletes all chunks for a specific filename."""
        with self.Session() as session:
            deleted = session.query(WineChunk).filter(
                WineChunk.meta_data['documental']['fuente_nombre'].astext == filename
            ).delete(synchronize_session=False)
            session.commit()
            print(f"Deleted {deleted} chunks for file: {filename}")
            return deleted

    def add_documents(self, chunks_data):
        """
        chunks_data: List of dicts {'embedding_text': str, 'metadata': dict, 'embedding': list[float]}
        """
        with self.Session() as session:
            for data in chunks_data:
                doc = WineChunk(
                    embedding_text=data['embedding_text'],
                    meta_data=data['metadata'],
                    embedding=data['embedding']
                )
                session.add(doc)
            session.commit()
        print(f"Added {len(chunks_data)} specialized wine records.")

    def search(self, query_embedding, limit=5):
        """
        Finds the nearest neighbors for the query embedding.
        """
        with self.Session() as session:
            results = session.query(WineChunk).order_by(
                WineChunk.embedding.cosine_distance(query_embedding)
            ).limit(limit).all()
            
            return [{
                'embedding_text': r.embedding_text,
                'metadata': r.meta_data,
                'distance': 0 
            } for r in results]
    def get_unique_labels(self):
        """Returns a list of unique wine labels based on vino_id."""
        with self.Session() as session:
            # We use a subquery to get distinct meta_data for each vino_id
            # Since multiple chunks share the same metadata, we pick one per vino_id
            from sqlalchemy import func
            subquery = session.query(
                func.min(WineChunk.id).label('min_id'),
                WineChunk.meta_data['identificacion']['vino_id'].astext.label('vino_id')
            ).group_by(
                WineChunk.meta_data['identificacion']['vino_id'].astext
            ).subquery()

            results = session.query(WineChunk).join(
                subquery, WineChunk.id == subquery.c.min_id
            ).all()

            return [{
                'id': r.id,
                'metadata': r.meta_data,
                'embedding_text_preview': r.embedding_text[:150] if r.embedding_text else ""
            } for r in results]

    def clear_all_chunks(self):
        """Deletes all records from the wine_chunks table."""
        with self.Session() as session:
            num_deleted = session.query(WineChunk).delete()
            session.commit()
            print(f"Database cleared: {num_deleted} chunks deleted.")
            return num_deleted
