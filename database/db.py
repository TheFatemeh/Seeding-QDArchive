"""SQLite database for storing dataset metadata using multi-table schema."""

import sqlite3
from pathlib import Path
from datetime import datetime


class Database:
    def __init__(self, db_path: str = "23724707-seeding.sqlite"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS PROJECTS (
                id                         INTEGER PRIMARY KEY,
                query_string               TEXT,
                repository_id              INTEGER NOT NULL,
                repository_url             TEXT NOT NULL,
                project_url                TEXT NOT NULL,
                version                    TEXT,
                title                      TEXT NOT NULL,
                description                TEXT NOT NULL,
                language                   TEXT,
                doi                        TEXT,
                upload_date                TEXT,
                download_date              TEXT NOT NULL,
                download_repository_folder TEXT NOT NULL,
                download_project_folder    TEXT NOT NULL,
                download_version_folder    TEXT,
                download_method            TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS FILES (
                id          INTEGER PRIMARY KEY,
                project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
                file_name   TEXT NOT NULL,
                file_type   TEXT NOT NULL,
                status      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS KEYWORDS (
                id          INTEGER PRIMARY KEY,
                project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
                keyword     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS PERSON_ROLE (
                id          INTEGER PRIMARY KEY,
                project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
                name        TEXT NOT NULL,
                role        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS LICENSES (
                id          INTEGER PRIMARY KEY,
                project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
                license     TEXT NOT NULL
            );
        """)
        self.conn.commit()

    def insert_project(self, **kwargs) -> int:
        """Insert a project record. Returns the project id."""
        kwargs.setdefault("download_date", datetime.now().isoformat())
        columns = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" * len(kwargs))
        cursor = self.conn.execute(
            f"INSERT INTO PROJECTS ({columns}) VALUES ({placeholders})",
            list(kwargs.values())
        )
        self.conn.commit()
        return cursor.lastrowid

    def insert_file(self, project_id: int, file_name: str, file_type: str, status: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO FILES (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
            (project_id, file_name, file_type, status)
        )
        self.conn.commit()
        return cursor.lastrowid

    def insert_keyword(self, project_id: int, keyword: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)",
            (project_id, keyword)
        )
        self.conn.commit()
        return cursor.lastrowid

    def insert_person_role(self, project_id: int, name: str, role: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)",
            (project_id, name, role)
        )
        self.conn.commit()
        return cursor.lastrowid

    def insert_license(self, project_id: int, license_str: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO LICENSES (project_id, license) VALUES (?, ?)",
            (project_id, license_str)
        )
        self.conn.commit()
        return cursor.lastrowid

    def project_exists(self, project_url: str) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM PROJECTS WHERE project_url = ?", (project_url,)
        )
        return cursor.fetchone() is not None

    def get_incomplete_downloads(self, repository_id: int) -> list:
        """Get projects that have files with non-SUCCEEDED status (for resume)."""
        cursor = self.conn.execute(
            """SELECT DISTINCT p.id, p.project_url, p.download_project_folder
               FROM PROJECTS p
               JOIN FILES f ON f.project_id = p.id
               WHERE p.repository_id = ?
                 AND f.status != 'SUCCEEDED'""",
            (repository_id,)
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_projects_without_files(self, repository_id: int) -> list:
        """Get projects that have no file records at all (for resume)."""
        cursor = self.conn.execute(
            """SELECT p.id, p.project_url, p.download_project_folder
               FROM PROJECTS p
               LEFT JOIN FILES f ON f.project_id = p.id
               WHERE p.repository_id = ?
                 AND f.id IS NULL""",
            (repository_id,)
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def update_project(self, project_id: int, **kwargs):
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        self.conn.execute(
            f"UPDATE PROJECTS SET {sets} WHERE id = ?",
            list(kwargs.values()) + [project_id]
        )
        self.conn.commit()

    def delete_files_for_project(self, project_id: int):
        self.conn.execute("DELETE FROM FILES WHERE project_id = ?", (project_id,))
        self.conn.commit()

    def delete_by_repository(self, repository_id: int):
        """Delete all records for a given repository across all tables."""
        self.conn.execute(
            "DELETE FROM FILES WHERE project_id IN (SELECT id FROM PROJECTS WHERE repository_id = ?)",
            (repository_id,)
        )
        self.conn.execute(
            "DELETE FROM KEYWORDS WHERE project_id IN (SELECT id FROM PROJECTS WHERE repository_id = ?)",
            (repository_id,)
        )
        self.conn.execute(
            "DELETE FROM PERSON_ROLE WHERE project_id IN (SELECT id FROM PROJECTS WHERE repository_id = ?)",
            (repository_id,)
        )
        self.conn.execute(
            "DELETE FROM LICENSES WHERE project_id IN (SELECT id FROM PROJECTS WHERE repository_id = ?)",
            (repository_id,)
        )
        self.conn.execute(
            "DELETE FROM PROJECTS WHERE repository_id = ?",
            (repository_id,)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
