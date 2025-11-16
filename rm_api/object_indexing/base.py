from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from rm_api import API


class ObjectIterWriter:
    def __init__(self, indexer: 'ObjectIndexer', fhash: str):
        self.indexer = indexer
        self.fhash = fhash
        self._data = bytearray()

    def write(self, data: bytes) -> None:
        self._data.extend(data)

    def __enter__(self) -> 'ObjectIterWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.indexer.write_bytes(self.fhash, bytes(self._data))


class ObjectIndexer(ABC):
    allow_write = True

    def __init__(self, api: 'API'):
        self.api = api
        self.writes_count = 0
        self.reads_count = 0
        self.unique_writes = set()
        self.unique_reads = set()

    @property
    def sync_path(self) -> Path:
        return Path(self.api.sync_file_path)

    def open_writer(self, fhash: str) -> ObjectIterWriter:
        """Open an iterator-based writer for the given hash."""
        return ObjectIterWriter(self, fhash)

    def write_file(self, fhash: str, obj: Union[str, bytes]) -> None:
        """Write a file with the given hash and data to the index."""
        if isinstance(obj, bytes):
            self.write_bytes(fhash, obj)
        elif isinstance(obj, str):
            self.write_string(fhash, obj)
        else:
            raise TypeError("obj must be of type str or bytes")

    def log_stats(self, enable_print: bool = True) -> None:
        """Print statistics about read and written objects."""
        self.api.log(
            f"Object Indexer Stats - "
            f"Written: {self.writes_count}, Read: {self.reads_count} | "
            f"Unique Writes: {len(self.unique_writes)}, Unique Reads: {len(self.unique_reads)}",
            enable_print=enable_print
        )

    def log_and_reset_stats(self, enable_print: bool = True) -> None:
        """Log statistics and reset counters."""
        self.log_stats()
        self.writes_count = 0
        self.reads_count = 0
        self.unique_writes.clear()
        self.unique_reads.clear()

    def register_write(self, fhash: str) -> None:
        """Register a write operation for the given hash."""
        self.writes_count += 1
        self.unique_writes.add(fhash)

    def register_read(self, fhash: str) -> None:
        """Register a read operation for the given hash."""
        self.reads_count += 1
        self.unique_reads.add(fhash)

    @abstractmethod
    def hash_exists(self, fhash: str) -> bool:
        """Check if an object with the given hash exists in the index."""
        ...

    @abstractmethod
    def write_bytes(self, fhash: str, data: bytes) -> None:
        """Write bytes data with the given hash to the index."""
        ...

    @abstractmethod
    def write_string(self, fhash: str, data: str) -> None:
        """Write string data with the given hash to the index."""
        ...

    @abstractmethod
    def read_bytes(self, fhash: str) -> bytes:
        """Read bytes data with the given hash from the index."""
        ...

    @abstractmethod
    def read_string(self, fhash: str) -> str:
        """Read string data with the given hash from the index."""
        ...

    @abstractmethod
    def get_size(self, fhash: str) -> int:
        """Get the size of the object with the given hash."""
        ...

    @abstractmethod
    def erase(self, fhash: str) -> None:
        """Erase the object with the given hash from the index."""
        ...
