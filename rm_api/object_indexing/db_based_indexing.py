from threading import Lock

import sqlalchemy as sa
from pathlib import Path
from typing import TYPE_CHECKING, Tuple, Dict

from sqlalchemy.orm import sessionmaker, scoped_session, DeclarativeBase

from .file_based_indexing import FileObjectIndexer

if TYPE_CHECKING:
    from rm_api import API


class Base(DeclarativeBase):
    pass


# A simple table definition for SQLAlchemy

class FileIndex(Base):
    __tablename__ = 'file_index'
    fhash = sa.Column(sa.String, primary_key=True)
    size = sa.Column(sa.Integer)
    bytes = sa.Column(sa.LargeBinary)


class DBObjectIndexer(FileObjectIndexer):
    def __init__(self, api: 'API'):
        super().__init__(api)
        self.file_cache = bytearray()
        self.file_cache_index: Dict[str, Tuple[int, int]] = {}
        self.init_lock = Lock()
        self.init_db()

    @property
    def Session(self):
        # Recreate the database if its missing
        if not (Path(self.api.sync_file_path) / "sync.db").exists():
            with self.init_lock:
                self.init_db()
        return self._Session

    def hash_exists(self, fhash: str) -> bool:
        with self.Session() as session:
            result = session.execute(sa.select(FileIndex).where(FileIndex.fhash == fhash))
            return result.first() is not None

    def write_bytes(self, fhash: str, data: bytes) -> None:
        assert isinstance(data, bytes)
        self.register_write(fhash)
        with self.Session() as session:
            file_index = session.get(FileIndex, fhash)
            if file_index is None:
                file_index = FileIndex(fhash=fhash, size=len(data), bytes=data)
                session.add(file_index)
            else:
                file_index.size = len(data)
                file_index.bytes = data

            session.commit()

    def write_string(self, fhash: str, data: str) -> None:
        self.write_bytes(fhash, data.encode('utf-8'))

    def read_bytes(self, fhash: str) -> bytes:
        self.register_read(fhash)
        with self.Session() as session:
            row = session.execute(
                sa.select(FileIndex.bytes).where(FileIndex.fhash == fhash)
            ).first()
            if row is None:
                raise FileNotFoundError(f"File with hash {fhash} not found in database.")
            data = row[0]
        return data

    def read_string(self, fhash: str) -> str:
        return self.read_bytes(fhash).decode('utf-8')

    def get_size(self, fhash: str) -> int:
        with self.Session() as session:
            result = session.execute(
                sa.select(FileIndex.size).where(FileIndex.fhash == fhash)
            )
            row = result.first()
            if row is None:
                raise FileNotFoundError(f"File with hash {fhash} not found in database.")
            return row[0]

    def erase(self, fhash: str) -> None:
        with self.Session() as session:
            session.execute(
                sa.delete(FileIndex).where(FileIndex.fhash == fhash)
            )
            session.commit()

    def init_db(self):
        engine = sa.create_engine(f"sqlite:///{self.api.sync_file_path}/sync.db",
                                  connect_args={"check_same_thread": False})
        with engine.begin() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL")

        with engine.begin() as conn:
            Base.metadata.create_all(conn)

        self._Session = sessionmaker(bind=engine, expire_on_commit=False)
