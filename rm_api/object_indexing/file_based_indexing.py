from pathlib import Path
from typing import TYPE_CHECKING, Tuple, Dict
from rm_api.object_indexing.base import ObjectIndexer

if TYPE_CHECKING:
    from rm_api import API

class FileObjectIndexer(ObjectIndexer):
    def __init__(self, api: 'API'):
        super().__init__(api)
        self.file_cache = bytearray()
        self.file_cache_index: Dict[str, Tuple[int, int]] = {}

    def hash_exists(self, fhash: str) -> bool:
        return (self.sync_path / fhash).exists()

    def write_bytes(self, fhash: str, data: bytes) -> None:
        self.register_write(fhash)
        with open(self.sync_path / fhash, 'wb') as f:
            f.write(data)

    def write_string(self, fhash: str, data: str) -> None:
        self.write_bytes(fhash, data.encode('utf-8'))

    def read_bytes(self, fhash: str) -> bytes:
        self.register_read(fhash)
        with open(self.sync_path / fhash, 'rb') as f:
            data = f.read()

        return data

    def read_string(self, fhash: str) -> str:
        return self.read_bytes(fhash).decode('utf-8')

    def get_size(self, fhash: str) -> int:
        return (self.sync_path / fhash).stat().st_size

    def erase(self, fhash: str) -> None:
        (self.sync_path / fhash).unlink()