from psycopg_pool import ConnectionPool
from src.config import DATABASE_URL

pool = ConnectionPool(conninfo=DATABASE_URL)

def get_connection():
    return pool.connection()