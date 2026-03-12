"""Simple SQLite database for storing dataset metadata."""

import sqlite3
from pathlib import Path
from datetime import datetime


class Database:
    def __init__(self, db_path: str = "archive_root/metadata/crawl.sqlite"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._create_table()

    def _create_table(self):
        """Create the datasets table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY,
                qda_file_url TEXT,
                download_timestamp TEXT NOT NULL,
                local_directory TEXT,
                qda_local_filename TEXT,
                repository_name TEXT,
                dataset_page_url TEXT,
                dataset_title TEXT,
                dataset_description TEXT,
                dataset_doi TEXT,
                keywords TEXT,
                download_method TEXT,
                download_status TEXT,
                file_count INTEGER,
                file_types TEXT,
                uploader_name TEXT,
                uploader_email TEXT,
                author_name TEXT,
                contact_person TEXT,
                license TEXT
            )
        """)
        self.conn.commit()

    def insert_dataset(self, **kwargs) -> int:
        """Insert a dataset record. Returns the row id."""
        # Set defaults for required fields
        kwargs.setdefault("download_timestamp", datetime.now().isoformat())
        
        columns = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" * len(kwargs))
        
        cursor = self.conn.execute(
            f"INSERT INTO datasets ({columns}) VALUES ({placeholders})",
            list(kwargs.values())
        )
        self.conn.commit()
        return cursor.lastrowid

    def dataset_exists(self, dataset_page_url: str) -> bool:
        """Check if a dataset with this page URL already exists."""
        cursor = self.conn.execute(
            "SELECT 1 FROM datasets WHERE dataset_page_url = ?", (dataset_page_url,)
        )
        return cursor.fetchone() is not None

    def get_incomplete_downloads(self, repository_name: str) -> list:
        """Get datasets where download is not successful (for resume).
        Returns list of dicts with id, dataset_page_url, download_status, file_count."""
        cursor = self.conn.execute(
            """SELECT id, dataset_page_url, download_status, file_count, local_directory
               FROM datasets
               WHERE repository_name = ?
                 AND download_status IN ('open', 'no_files', 'failed')""",
            (repository_name,)
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def update_download(self, dataset_id: int, **kwargs):
        """Update download fields for a dataset."""
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        self.conn.execute(
            f"UPDATE datasets SET {sets} WHERE id = ?",
            list(kwargs.values()) + [dataset_id]
        )
        self.conn.commit()

    def delete_by_repository(self, repository_name: str):
        """Delete all records for a given repository."""
        self.conn.execute(
            "DELETE FROM datasets WHERE repository_name = ?",
            (repository_name,)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
