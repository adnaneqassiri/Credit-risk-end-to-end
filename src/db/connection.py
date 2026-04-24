from psycopg_pool import ConnectionPool
from src.config import DATABASE_URL

pool = None


def get_pool():
    global pool
    if pool is None:
        pool = ConnectionPool(conninfo=DATABASE_URL, open=False)
        pool.open()
    return pool


def get_connection():
    return get_pool().connection()
