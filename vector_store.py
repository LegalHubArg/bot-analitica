import os
import json
from sqlalchemy import create_engine, Column, Integer, String, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class DocumentChunk(Base):
    __tablename__ = 'document_chunks'

    id = Column(Integer, primary_key=True)
    content = Column(Text)
    meta_data = Column(JSONB)  # 'metadata' is reserved in Base
    embedding = Column(Vector(1536))

class VectorStore:
    def __init__(self):
        # Expecting DATABASE_URL env var
        # For Cloud Run: postgresql+psycopg2://user:pass@/dbname?host=/cloudsql/instance_connection_name
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set")

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def init_db(self):
        """Creates the vector extension and tables if they don't exist."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()
        
        Base.metadata.create_all(self.engine)
        print("Database initialized (extension + tables).")

    def clear_documents(self):
        """Clears all documents from the store."""
        with self.Session() as session:
            session.query(DocumentChunk).delete()
            session.commit()
        print("All documents cleared.")
    
    def get_indexed_files_info(self):
        """Returns a dictionary of {filename: modified_time} for all indexed files."""
        with self.Session() as session:
            # We query the distinct pairs of filename and modified_at from metadata
            results = session.query(
                DocumentChunk.meta_data['filename'].astext,
                DocumentChunk.meta_data['modified_at'].astext
            ).distinct().all()
            return {r[0]: r[1] for r in results if r[0]}
    
    def delete_by_filename(self, filename):
        """Deletes all chunks for a specific filename."""
        with self.Session() as session:
            deleted = session.query(DocumentChunk).filter(
                DocumentChunk.meta_data['filename'].astext == filename
            ).delete(synchronize_session=False)
            session.commit()
            print(f"Deleted {deleted} chunks for file: {filename}")
            return deleted

    def add_documents(self, chunks_data):
        """
        chunks_data: List of dicts {'content': str, 'metadata': dict, 'embedding': list[float]}
        """
        with self.Session() as session:
            for data in chunks_data:
                doc = DocumentChunk(
                    content=data['content'],
                    meta_data=data['metadata'],
                    embedding=data['embedding']
                )
                session.add(doc)
            session.commit()
        print(f"Added {len(chunks_data)} documents.")

    def search(self, query_embedding, limit=5):
        """
        Finds the nearest neighbors for the query embedding.
        """
        with self.Session() as session:
            # L2 distance ( <-> op). For inner product use <#> or cosine use <=>
            # Usually cosine distance (<=>) is best for embeddings, but we need to order by distance ASC
            results = session.query(DocumentChunk).order_by(
                DocumentChunk.embedding.cosine_distance(query_embedding)
            ).limit(limit).all()
            
            return [{
                'content': r.content,
                'metadata': r.meta_data,
                'distance': 0 # We could fetch distance but keeping it simple
            } for r in results]
