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
        # Expecting DATABASE_URL env var
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set")

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
