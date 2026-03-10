from contextlib import contextmanager
from typing import Generator, Optional
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from loguru import logger

from .config import get_settings


_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def get_engine() -> Engine:
    """Get or create SQLAlchemy engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            poolclass=NullPool,
            echo=False,
            connect_args={"connect_timeout": 10},
        )
        logger.info(f"Created SQLAlchemy engine for {settings.database_url}")
    return _engine


def get_session_factory() -> sessionmaker:
    """Get or create session factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    return _session_factory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager for database sessions."""
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Session error: {e}")
        raise
    finally:
        session.close()


def get_raw_connection():
    """Get raw psycopg2 connection for bulk operations."""
    settings = get_settings()
    try:
        # Parse database URL to get connection parameters
        from urllib.parse import urlparse
        parsed = urlparse(settings.database_url)

        conn = psycopg2.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=parsed.path.lstrip("/"),
            user=parsed.username or "chuser",
            password=parsed.password or "chpass",
            connect_timeout=10,
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to get raw connection: {e}")
        raise


@contextmanager
def get_raw_connection_context():
    """Context manager for raw psycopg2 connections."""
    conn = get_raw_connection()
    try:
        yield conn
    finally:
        conn.close()


def execute_raw_sql(query: str, params: tuple = ()):
    """Execute raw SQL query."""
    with get_raw_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            if cur.description:
                return cur.fetchall()
    return None


def execute_raw_sql_with_fetch(query: str, params: tuple = ()):
    """Execute raw SQL query and fetch results."""
    with get_raw_connection_context() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()


def bulk_copy_csv(table_name: str, csv_data: str, columns: list[str]):
    """Bulk copy CSV data into table using COPY command."""
    with get_raw_connection_context() as conn:
        with conn.cursor() as cur:
            column_list = ", ".join(columns)
            copy_sql = f"COPY {table_name} ({column_list}) FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')"
            cur.copy_expert(copy_sql, csv_data)
            conn.commit()
            logger.info(f"Bulk copied data into {table_name}")
