import os
from vector_store import VectorStore
from dotenv import load_dotenv

load_dotenv()

def main():
    try:
        print("Iniciando conexión con la base de datos...")
        vs = VectorStore()
        vs.init_db()
        print("¡Tabla 'wine_chunks' creada/verificada exitosamente!")
    except Exception as e:
        print(f"Error al inicializar la base de datos: {e}")

if __name__ == "__main__":
    main()
