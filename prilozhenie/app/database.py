import os
from dotenv import load_dotenv
import psycopg2
from contextlib import contextmanager

# Загрузка переменных окружения (для локальной разработки)
if os.path.exists('.env'):
    load_dotenv()

class Database:
    @staticmethod
    def get_connection():
        """Получение подключения к базе данных"""
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database=os.getenv("DB_NAME", "library_management"),
                user=os.getenv("DB_USER", "admin"),
                password=os.getenv("DB_PASSWORD", "123"),
                port=os.getenv("DB_PORT", "5432")
            )
            return conn
        except Exception as e:
            print(f"❌ Ошибка подключения к базе данных: {e}")
            raise
    
    @staticmethod
    @contextmanager
    def get_connection_context():
        """Контекстный менеджер для подключения к БД"""
        conn = None
        try:
            conn = Database.get_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()