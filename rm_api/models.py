import concurrent.futures
import json
import os.path
import threading
import time
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy, deepcopy
from datetime import datetime
from functools import lru_cache
from hashlib import sha256
from io import BytesIO
from itertools import zip_longest
from typing import List, TYPE_CHECKING, Generic, T, Union, TypedDict, Dict, Optional, Tuple, Any

from rm_api.defaults import RM_SCREEN_CENTER, RM_SCREEN_SIZE, ZoomModes, Orientations, DocumentTypes
from rm_api.helpers import get_pdf_page_count, DownloadOperationsSupport
from rm_api.notifications.models import APIFatal, DownloadOperation, DocumentDownloadProgress
from rm_api.storage.common import FileHandle
from rm_api.storage.v3 import get_file_contents, CacheMiss
from rm_api.templates import BLANK_TEMPLATE
from rm_api.sync_stages import DOWNLOAD_CONTENT, FETCH_FILE

try:
    from rm_lines.rmscene.scene_stream import write_blocks
    from rm_lines.writer import blank_document
except ModuleNotFoundError:
    write_blocks = blank_document = None

if TYPE_CHECKING:
    from rm_api import API


def now_time_int():
    return int(time.time() * 1000)


def now_time():
    return str(now_time_int())


def make_uuid():
    return str(uuid.uuid4())


def make_hash(data: Union[str, bytes, FileHandle, dict]):
    if isinstance(data, FileHandle):
        return data.hash()
    if isinstance(data, str):
        return sha256(data.encode()).hexdigest()
    if isinstance(data, dict):
        return sha256(json.dumps(data, indent=4).encode()).hexdigest()
    return sha256(data).hexdigest()


def try_to_load_int(rm_value: Union[str, int], default: int = 0):
    if not rm_value:
        return default
    elif isinstance(rm_value, int):
        return rm_value
    else:
        return int(rm_value)


class File:
    def __init__(self, file_hash: str, file_uuid: str, content_count: str, file_size: Union[str, int],
                 rm_filename=None):
        self.hash = file_hash
        self.uuid = file_uuid
        self.content_count = try_to_load_int(content_count)
        if isinstance(file_size, str):
            self.size = try_to_load_int(file_size)
        else:
            self.size = file_size
        self.rm_filename = rm_filename or file_uuid

    @classmethod
    def create_root_file(cls, files: List['File']) -> Tuple[bytes, 'File']:
        root_file_content = ['3\n']
        root_file_hash = sha256()
        for file in sorted(files, key=lambda _: _.uuid):
            root_file_content.append(file.to_root_line())
            root_file_hash.update(bytes.fromhex(file.hash))

        root_file_content = ''.join(root_file_content).encode()
        root_file = File(root_file_hash.hexdigest(), f"root.docSchema", len(files), len(root_file_content))
        return root_file_content, root_file

    @classmethod
    def from_line(cls, line):
        file_hash, _, file_uuid, content_count, file_size = line.split(':')
        return cls(file_hash, file_uuid, content_count, file_size)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(data['hash'], data['uuid'], data['content_count'], data['file_size'], data.get('rm_filename'))

    def to_root_line(self):
        return f'{self.hash}:80000000:{self.uuid}:{self.content_count}:{self.size}\n'

    def to_line(self):
        return f'{self.hash}:0:{self.uuid}:{self.content_count}:{self.size}\n'

    def update_document_file(self, api: 'API', files: List['File'], content_datas: Dict[str, Any]) -> bytes:
        document_file_content = ['3\n']
        document_file_hash = sha256()
        self.size = 0
        for file in sorted(files, key=lambda file: file.uuid):
            if data := content_datas.get(file.uuid):
                file.hash = make_hash(data)
                file.size = len(data)
            elif file.uuid.endswith('.content') or file.uuid.endswith('.metadata'):
                api.log(f"File {file.uuid} not found in content data: {file.hash}")
                api.spread_event(APIFatal())

            document_file_hash.update(bytes.fromhex(file.hash))
            self.size += file.size

            document_file_content.append(file.to_line())

        document_file_content = ''.join(document_file_content).encode()
        self.hash = document_file_hash.hexdigest()
        return document_file_content

    def save_to_cache(self, api: 'API', data: bytes):
        location = os.path.join(api.sync_file_path, self.hash)
        if os.path.exists(location):
            return  # No need cache it if it is already cached
        with open(location, 'wb') as f:
            f.write(data)

    def __repr__(self):
        return f'{self.uuid} ({self.size})[{self.content_count}]'

    def __str__(self):
        return self.__repr__()

    # Parse and re-unparse the file to make a copy
    def __copy__(self):
        return self.from_line(self.to_line())

    def __deepcopy__(self, memo=None):
        return self.from_line(self.to_line())


class TimestampedValue(Generic[T]):
    def __init__(self, value: dict):
        self.value: T = value['value']
        self.timestamp: str = value['timestamp']

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'value': self.value
        }

    @classmethod
    def create(cls, value: T, t1: int = 1, t2: int = 1, bare: bool = False) -> Union[dict, 'TimestampedValue']:
        dictionary = {'timestamp': f'{t1}:{t2}', 'value': value}
        if bare:
            return dictionary
        return cls(dictionary)

    @property
    def __dict__(self):
        return self.to_dict()


class TimestampedDate(TimestampedValue[int]):
    def __init__(self, value: dict):
        value['value']: int = int(datetime.strptime(value['value'], "%Y-%m-%dT%H:%M:%SZ").timestamp())
        super().__init__(value)

    def to_dict(self):
        result = super().to_dict()
        return {
            **result,
            'value': datetime.fromtimestamp(result['value']).strftime("%Y-%m-%dT%H:%M:%SZ")
        }


class Page:
    id: str  # This is technically the UUID, a .rm file may or may not exist for this page
    index: TimestampedValue[str]
    template: TimestampedValue[str]
    redirect: Optional[TimestampedValue[int]]
    scroll_time: Optional[TimestampedDate]
    vertical_scroll: Optional[TimestampedValue[int]]

    def __init__(self, page: dict):
        self.__page = page
        self.id = page['id']
        self.index: TimestampedValue[str] = TimestampedValue(page['idx'])
        if template := page.get('template'):
            self.template: TimestampedValue[str] = TimestampedValue(page['template'])
        else:
            self.template = TimestampedValue[str].create(BLANK_TEMPLATE)

        # Check for a redirect
        # If the document is not a notebook this will be on every page
        # Except any user created pages
        if redirect := page.get('redir'):
            self.redirect = TimestampedValue(redirect)
        else:
            self.redirect = None

        if scroll_time := page.get('scrollTime'):
            self.scroll_time = TimestampedDate(scroll_time)
        else:
            self.scroll_time = None

        if vertical_scroll := page.get('verticalScroll'):
            self.vertical_scroll = TimestampedValue(vertical_scroll)
        else:
            self.vertical_scroll = None

    @staticmethod
    def new_page_dict(index: str, page_uuid: str = None):
        return {
            "id": page_uuid if page_uuid else make_uuid(),
            "idx": {
                "timestamp": "1:2",
                "value": index
            }
        }

    @classmethod
    def new_page(cls, index: str, page_uuid: str = None):
        return cls(cls.new_page_dict(index, page_uuid))

    @classmethod
    def new_pdf_redirect_dict(cls, redirection_page: int, index: str, page_uuid: str = None):
        return {
            **cls.new_page_dict(index, page_uuid),
            "redir": {
                "timestamp": "1:2",
                "value": redirection_page
            }
        }

    @classmethod
    def new_pdf_redirect(cls, redirection_page: int, index: str, page_uuid: str = None):
        return cls(cls.new_pdf_redirect_dict(redirection_page, index, page_uuid))

    def to_dict(self):
        result = {
            'id': self.id,
            'idx': self.index.to_dict(),
        }
        if self.template:
            result['template'] = self.template.to_dict()
        if self.redirect:
            result['redir'] = self.redirect.to_dict()

        return result

    @property
    def __dict__(self):
        result = {
            'id': self.id,
            'index': self.index.__dict__,
            'template': self.template.__dict__,
        }
        if self.redirect:
            result['redirect'] = self.redirect.__dict__
        if self.scroll_time:
            result['scroll_time'] = self.scroll_time.__dict__
        if self.vertical_scroll:
            result['vertical_scroll'] = self.vertical_scroll.__dict__
        return result


# TODO: Figure out what the CPagesUUID is referring to
class CPagesUUID(TypedDict):
    first: str
    second: int


class CPages:
    pages: List[Page]
    original: TimestampedValue[int]  # Seems to reference the original page count
    last_opened: TimestampedValue[str]  # The id of the last opened page
    uuids: List[CPagesUUID]

    def __init__(self, c_pages: dict):
        self.__c_pages = c_pages
        self.pages = [Page(page) for page in c_pages['pages'] if not page.get('deleted')]
        self.original = TimestampedValue(c_pages['original'])
        self.last_opened = TimestampedValue(c_pages['lastOpened'])
        self.uuids = c_pages['uuids']

    def get_index_from_uuid(self, uuid: str):
        for i, page in enumerate(self.pages):
            if page.id == uuid:
                return i
        return None

    @lru_cache(maxsize=20)
    def get_page_from_uuid(self, uuid: str):
        for page in self.pages:
            if page.id == uuid:
                return page
        return None

    def to_dict(self) -> dict:
        return {
            'lastOpened': self.last_opened.to_dict(),
            'original': self.original.to_dict(),
            'pages': [page.to_dict() for page in self.pages],
            'uuids': self.uuids
        }

    @property
    def __dict__(self):
        return {
            'pages': [page.__dict__ for page in self.pages],
            'original': self.original.__dict__,
            'last_opened': self.last_opened.__dict__,
            'uuids': self.uuids,
        }


class Zoom:
    ZOOM_TEMPLATE = {
        "customZoomCenterX": 0,
        "customZoomCenterY": RM_SCREEN_CENTER[1],
        "customZoomOrientation": "portrait",
        "customZoomPageHeight": RM_SCREEN_SIZE[1],
        "customZoomPageWidth": RM_SCREEN_SIZE[0],
        "customZoomScale": 1,
    }

    def __init__(self, content):
        zoom_mode = content.get('zoomMode', None)
        self._zoom_mode = ZoomModes(zoom_mode) if zoom_mode else None
        if not self._zoom_mode:
            content = self.ZOOM_TEMPLATE
        self.custom_zoom_center_x = content['customZoomCenterX']
        self.custom_zoom_center_y = content['customZoomCenterY']
        self.custom_zoom_page_width = content['customZoomPageWidth']
        self.custom_zoom_page_height = content['customZoomPageHeight']
        self.custom_zoom_scale = content['customZoomScale']

    @property
    def zoom_mode(self):
        return self._zoom_mode or ZoomModes.BestFit

    def to_dict(self):
        return {
            'zoomMode': self.zoom_mode.value,
            "customZoomCenterX": self.custom_zoom_center_x,
            "customZoomCenterY": self.custom_zoom_center_y,
            "customZoomOrientation": "portrait",
            "customZoomPageHeight": self.custom_zoom_page_height,
            "customZoomPageWidth": self.custom_zoom_page_width,
            "customZoomScale": self.custom_zoom_scale,
        }

    @property
    def __dict__(self):
        return self.to_dict()


class Content:
    """
    This class only represents the content data
    of a document and not a document collection.

    EXPLANATION:
        The content of a document collection is much more simple,
        it only contains tags: List[Tag]
        This is handled by the parser and handed to the document collection,
        So this class is only for the content of a document.
    """

    hash: str
    c_pages: Union[CPages, None]
    cover_page_number: int
    file_type: str
    version: int
    CONTENT_TEMPLATE = {
        "dummyDocument": False,
        "extraMetadata": {
            "LastPen": "Finelinerv2",
            "LastTool": "Finelinerv2",
            "ThicknessScale": "",
            "LastFinelinerv2Size": "1"
        },
        "fontName": "",
        "lastOpenedPage": 0,
        "lineHeight": -1,
        "margins": 180,
        "orientation": "portrait",
        "pageCount": 0,
        "textScale": 1,
        "formatVersion": 1,
        "transform": {
            "m11": 1,
            "m12": 0,
            "m13": 0,
            "m21": 0,
            "m22": 1,
            "m23": 0,
            "m31": 0,
            "m32": 0,
            "m33": 1
        }
    }
    PDF_CONTENT_TEMPLATE = {
        "fileType": "pdf",
        **CONTENT_TEMPLATE,
    }
    EPUB_CONTENT_TEMPLATE = {
        "fileType": "epub",
        **CONTENT_TEMPLATE,
    }

    def __init__(self, content: dict, metadata: Optional['Metadata'], content_hash: str, show_debug: bool = False):
        self._content = content
        self._metadata = metadata
        self.hash = content_hash
        self.usable = True
        self.c_pages: CPages = None
        self.content_file_pdf_check = False  # There is a pdf but the pages aren't registered in the content
        self.cover_page_number: int = content.get('coverPageNumber', 0)
        self.dummy_document: bool = content.get('dummyDocument', False)
        self.file_type: str = content['fileType']
        self.version: int = content.get('formatVersion')
        self.size_in_bytes: int = try_to_load_int(content.get('sizeInBytes'), -1)
        self.tags: List[Tag] = [Tag(tag) for tag in content.get('tags', ())]
        self.zoom = Zoom(content)
        self.orientation: str = content.get('orientation', 'portrait')

        # Handle parsing the different versions
        if self.version == 2:
            self.parse_version_2()
            return
        elif self.version == 1:
            self.parse_version_1(show_debug)
            return
        if not self.version:
            # Try to parse content version 1 if there is no version
            try:
                self.parse_version_1(show_debug)
            except KeyError:
                # Fails to parse as version 1, just fail cause the version is missing
                self.usable = False
                if show_debug:
                    print(f'{Fore.RED}Content file version is missing{Fore.RESET}')
        else:
            # Fail because the version is something else than can be parsed
            self.usable = False
            if show_debug:
                print(f'{Fore.YELLOW}Content file version is unknown: {self.version}{Fore.RESET}')

    @property
    def is_landscape(self):
        return self.orientation == Orientations.Landscape.value

    @property
    def is_portrait(self):
        return self.orientation == Orientations.Portrait.value

    def parse_version_2(self):
        self.c_pages = CPages(self._content['cPages'])

    def parse_version_1(self, show_debug: bool = False):
        self.version = 2  # promote to version 2
        # Handle error checking since a lot of these can be empty
        try:
            original_page_count = self._content.pop('originalPageCount')
        except KeyError:
            original_page_count = 0
        try:
            pages = self._content.pop('pages')
        except KeyError:
            pages = None
        if not pages:
            pages = []
            self.content_file_pdf_check = True
        try:
            redirection_page_map = self._content.pop('redirectionPageMap')
        except KeyError:
            redirection_page_map = []
        index = self.page_index_generator()
        c_page_pages = []
        last_opened_page = None
        for i, (page, redirection_page) in enumerate(zip_longest(pages, redirection_page_map, fillvalue=-2)):
            if redirection_page != -1:
                c_page_pages.append(Page.new_pdf_redirect_dict(redirection_page, next(index), page))
            elif redirection_page != -2:
                c_page_pages.append(Page.new_pdf_redirect_dict(i, next(index), page))
            else:
                c_page_pages.append(Page.new_page_dict(next(index), page))
            if i == self._metadata.last_opened_page:
                last_opened_page = page
            if i == self._content.get('lastOpenedPage'):
                last_opened_page = page

        self.c_pages = CPages(
            {
                'pages': c_page_pages,
                'original': TimestampedValue.create(original_page_count, bare=True),
                'lastOpened': TimestampedValue.create(last_opened_page, bare=True),
                'uuids': [{
                    'first': make_uuid(),  # Author
                    'second': 1
                }]
            }
        )

    @classmethod
    def new_notebook(cls, author_id: Optional[str] = None, page_count: int = 1):
        first_page_uuid = make_uuid()
        if not author_id:
            author_id = make_uuid()
        page_index = cls.page_index_generator()
        content = {
            'cPages': {
                'lastOpened': TimestampedValue[str].create(first_page_uuid, bare=True),
                'original': TimestampedValue[int].create(-1, 0, 0, bare=True),
                'pages': [{
                    'id': first_page_uuid if i == 0 else make_uuid(),
                    'idx': TimestampedValue[str].create(next(page_index), t2=2, bare=True),
                    'template': TimestampedValue[str].create(BLANK_TEMPLATE, bare=True),
                } for i in range(page_count)],
                'uuids': [{
                    'first': author_id,  # This is the author id
                    'second': 1
                }]
            },
            "coverPageNumber": 0,
            "customZoomCenterX": 0,
            "customZoomCenterY": 936,
            "customZoomOrientation": "portrait",
            # rM2 page size
            # Q: Check values on RPP and if zoom changes
            # A: It actually does not, even for new notebooks
            "customZoomPageHeight": 1872,
            "customZoomPageWidth": 1404,
            "customZoomScale": 1,
            "documentMetadata": {},
            "extraMetadata": {},
            "fileType": "notebook",
            "fontName": "",
            "formatVersion": 2,
            "lineHeight": -1,
            "margins": 125,
            "orientation": "portrait",
            "pageCount": 1,
            "pageTags": [],
            "sizeInBytes": "0",  # This is not important here
            "tags": [],
            "textAlignment": "justify",
            "textScale": 1,
            "zoomMode": "bestFit"
        }
        return cls(content, None, make_hash(json.dumps(content, indent=4)))

    @classmethod
    def new_pdf(cls):
        return cls(cls.PDF_CONTENT_TEMPLATE, None, make_hash(json.dumps(cls.PDF_CONTENT_TEMPLATE, indent=4)))

    @classmethod
    def new_epub(cls):
        return cls(cls.EPUB_CONTENT_TEMPLATE, None, make_hash(json.dumps(cls.EPUB_CONTENT_TEMPLATE, indent=4)))

    def to_dict(self) -> dict:
        return {
            **self.CONTENT_TEMPLATE,
            **self._content,
            **self.zoom.to_dict(),
            'fileType': self.file_type,
            'formatVersion': self.version,
            'cPages': self.c_pages.to_dict(),
            'tags': [tag.to_rm_json() for tag in self.tags],
            'sizeInBytes': str(self.size_in_bytes),
            'coverPageNumber': self.cover_page_number,
        }

    @property
    def __dict__(self):
        return {
            'hash': self.hash,
            'c_pages': self.c_pages.__dict__,
            'cover_page_number': self.cover_page_number,
            'file_type': self.file_type,
            'version': self.version,
            'usable': self.usable,
            'zoom': self.zoom.__dict__,
            'orientation': self.orientation,
            'tags': [tag.__dict__ for tag in self.tags],
            'size_in_bytes': self.size_in_bytes,
            'dummy_document': self.dummy_document,
        }

    def __str__(self):
        return f'content version: {self.version} file type: {self.file_type}'

    @staticmethod
    def page_index_generator():
        char_first = chr(ord('a') - 1)
        chars = ['b', char_first]
        target = 1
        flag_z = 0

        def increment_char(char):
            return chr(ord(char) + 1)

        while True:
            chars[target] = increment_char(chars[target])

            do_n = chars[target - flag_z] == 'n' if flag_z else chars[target] == 'n'
            if do_n:
                n_count = 0
                n_index = target - flag_z if flag_z else target
                while n_index >= 0:
                    if chars[n_index] == 'n':
                        n_count += 1
                    else:
                        break
                    n_index -= 1
                if n_index < 0 and n_count >= 2:
                    yield ''.join([*chars, 'a'])
                    chars.extend(('b', 'a'))
                    target += 2

            yield ''.join(chars)

            z_index = target
            flag_z = 0
            while chars[z_index] == 'z':
                if z_index == 0:
                    chars.insert(0, 'a')
                    target += 1
                    z_index += 1
                chars[z_index] = char_first if z_index == target else 'a'
                if chars[z_index - 1] == 'z':
                    z_index -= 1
                else:
                    chars[z_index - 1] = increment_char(chars[z_index - 1])

                flag_z += 1

    def check(self, document: 'Document'):
        if self.content_file_pdf_check and self.file_type == 'pdf':
            try:
                self.parse_create_new_pdf_content_file(document)
                self.content_file_pdf_check = False
            except KeyError:  # If files are missing for whatever reason
                pass
        elif self.file_type == 'epub' and len(self.c_pages.pages) == 0:
            self.usable = False
        size = 0
        for file in document.files:
            if file.uuid in document.content_data:
                size += len(document.content_data[file.uuid])
        self.size_in_bytes = size

    def _parse_create_new_pdf_content_file(self, pdf: bytes):
        page_count = get_pdf_page_count(pdf)

        index = self.page_index_generator()
        self.c_pages.pages = [
            Page.new_pdf_redirect(i, next(index))
            for i in range(page_count)
        ]
        self.c_pages.original.value = page_count

    def parse_create_new_pdf_content_file(self, document: 'Document'):
        """Creates the c_pages data for a pdf that wasn't indexed"""
        pdf = document.content_data[f'{document.uuid}.pdf']

        self._parse_create_new_pdf_content_file(pdf)


class Metadata:
    def __init__(self, metadata: dict, metadata_hash: str):
        self._metadata = metadata
        self.hash = metadata_hash
        self.type = metadata['type']
        self.parent = metadata['parent'] or None
        self.pinned = metadata['pinned']  # Pinned is equivalent to starred
        self.created_time = try_to_load_int(metadata.get('createdTime'))
        self.last_modified = try_to_load_int(metadata['lastModified'])
        self.visible_name = metadata['visibleName']
        self.metadata_modified = metadata.get('metadatamodified', False)
        self.modified = metadata.get('modified', False)
        self.synced = metadata.get('synced', False)
        self.version = metadata.get('version')

        if self.type == 'DocumentType':
            self.last_opened = try_to_load_int(metadata['lastOpened'])
            self.last_opened_page = metadata.get('lastOpenedPage', 0)

    @classmethod
    def new(cls, name: str, parent: Optional[str] = None, document_type: str = DocumentTypes.Document.value):
        now = now_time()
        metadata = {
            "deleted": False,
            "lastModified": now,
            "createdTime": now,
            "lastOpened": "",
            "lastOpenedPage": 0,
            "metadatamodified": True,
            "modified": False,
            "parent": parent or '',
            "pinned": False,
            "synced": True,
            "type": document_type,
            "version": 1,
            "visibleName": name
        }
        return cls(metadata, make_hash(json.dumps(metadata, indent=4)))

    def __setattr__(self, key, value):
        super().__setattr__(key, value)

        # A dirty translation of the keys to metadata keys
        if key == 'created_time':
            key = 'createdTime'
            value = str(value)
        if key == 'last_modified':
            key = 'lastModified'
            value = str(value)
        if key == 'visible_name':
            key = 'visibleName'
        if key == 'metadata_modified':
            key = 'metadatamodified'
        if key == 'last_opened':
            key = 'lastOpened'
            value = str(value)
        if key == 'last_opened_page':
            key = 'lastOpenedPage'

        if key not in self._metadata:
            return

        self._metadata[key] = value

    def to_dict(self) -> dict:
        return {
            **self._metadata,
            'parent': self._metadata['parent'] or ''
        }

    @property
    def __dict__(self):
        return {
            "created_time": self.created_time,
            "hash": self.hash,
            "last_modified": self.last_modified,
            "metadata_modified": self.metadata_modified,
            "modified": self.modified,
            "parent": self.parent,
            "pinned": self.pinned,
            "synced": self.synced,
            "type": self.type,
            "version": self.version,
            "visible_name": self.visible_name,
            **(
                {
                    "last_opened": self.last_opened,
                    "last_opened_page": self.last_opened_page
                } if self.type == 'DocumentType' else {}
            )
        }

    def modify_now(self):
        self.last_modified = now_time_int()


class Tag:
    def __init__(self, tag):
        self.name = tag['name']
        self.timestamp = tag['timestamp']

    def to_rm_json(self):
        return {
            'name': self.name,
            'timestamp': self.timestamp
        }

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class DocumentCollection:
    downloading = False
    available = True

    def __init__(self, tags: List[Tag], metadata: Metadata, uuid: str):
        self.tags = tags
        self.metadata = metadata
        self.uuid = uuid
        self.has_items = False

    @property
    def parent(self):
        return self.metadata.parent

    @parent.setter
    def parent(self, value):
        self.metadata.parent = value

    @property
    def content(self):
        return json.dumps({
            'tags': [tag.to_rm_json() for tag in self.tags]
        }, indent=4)

    @property
    def files(self):
        content_data = self.content_data
        metadata = content_data[f'{self.uuid}.metadata']
        content = content_data[f'{self.uuid}.content']
        return [
            File(make_hash(metadata), f'{self.uuid}.metadata', 0, len(metadata)),
            File(make_hash(content), f'{self.uuid}.content', 0, len(content)),
        ]

    @property
    def content_data(self):
        return {
            f'{self.uuid}.metadata': json.dumps(self.metadata.to_dict(), indent=4).encode(),
            f'{self.uuid}.content': self.content.encode()
        }

    def __repr__(self):
        return f'{self.metadata.visible_name}'

    @classmethod
    def create(cls, api: 'API', name: str, parent: str = None, document_uuid: str = None):
        if not document_uuid:
            document_uuid = make_uuid()
        return cls([], Metadata.new(name, parent, DocumentTypes.Collection.value), document_uuid)

    def ensure_download(self):
        pass

    def ensure_download_and_callback(self, callback):
        callback()

    def export(self):
        pass

    def check(self):
        pass

    def check_files_availability(self):
        return {}

    def unload_files(self):
        pass

    def recurse(self, api: 'API'):
        """Recursively get all the documents in the collection"""
        items = []
        for document in dict(api.documents).values():
            if document.parent == self.uuid:
                items.append(document)
        for collection in dict(api.document_collections).values():
            if collection.parent == self.uuid:
                items.extend(collection.recurse(api))
                items.append(collection)
        return items

    def get_item_count(self, api: 'API'):
        """Get the number of items in the collection"""
        count = 0
        for document in dict(api.documents).values():
            if document.parent == self.uuid:
                count += 1
        for collection in dict(api.document_collections).values():
            if collection.parent == self.uuid:
                count += 1
        return count

    @classmethod
    def __copy(cls, document_collection: 'DocumentCollection', shallow: bool = True):
        # Duplicate content and metadata
        tags = [
            Tag(tag.to_rm_json()) for tag in document_collection.tags
        ]
        raw_metadata = document_collection.metadata.to_dict()
        metadata = Metadata(raw_metadata, make_hash(json.dumps(raw_metadata, indent=4)))

        new = cls(tags, metadata, document_collection.uuid)
        return new

    def __copy__(self):
        return self.__copy(self)

    def __deepcopy__(self, memo=None):
        return self.__copy(self, shallow=False)

    @property
    def __dict__(self):
        return {
            'uuid': self.uuid,
            'metadata': self.metadata.__dict__,
            'tags': [tag.__dict__ for tag in self.tags],
            'has_items': self.has_items
        }

    def duplicate(self, api: 'API'):
        my_items: List[Union[Document, DocumentCollection]] = []
        my_copy = deepcopy(self)
        my_copy.uuid = make_uuid()
        my_copy.metadata.last_modified = now_time_int()
        my_copy.metadata.created_time = now_time_int()
        for document in reversed(api.documents.values()):
            if document.parent == self.uuid:
                my_items.append(document.duplicate())
                my_items[-1].parent = my_copy.uuid
        for document_collection in reversed(api.document_collections.values()):
            if document_collection.parent == self.uuid:
                sub_items, sub_copy = document_collection.duplicate(api)
                my_items.extend(sub_items)
                sub_copy.parent = my_copy.uuid
                my_items.append(sub_copy)
        return my_items, my_copy


class Document(DownloadOperationsSupport):
    unknown_file_types = set()
    KNOWN_FILE_TYPES = [
        'pdf', 'notebook', 'epub'
    ]
    CONTENT_FILE_TYPES = [
        'pdf', 'rm', 'epub', 'pagedata', '-metadata.json'
    ]

    files: List[File]
    content_data: Dict[str, bytes]

    def __init__(self, api: 'API', content: Content, metadata: Metadata, files: List[File], uuid: str,
                 server_hash: str = None, check: bool = True):
        super().__init__()
        self.api = api
        self.content = content
        self.metadata = metadata
        self.files = files
        self._uuid = uuid
        self.server_hash = server_hash
        self.content_data = {}
        self.files_available: Dict[str, File] = self.check_files_availability()
        self.provision = False  # Used during sync to disable opening or exporting the file!!!
        self.download_progress = DocumentDownloadProgress(self)

        if self.content.file_type not in self.KNOWN_FILE_TYPES and \
                not self.content.file_type in self.unknown_file_types:
            self.unknown_file_types.add(self.content.file_type)
            print(f'{Fore.RED}Unknown file type: {self.content.file_type}{Fore.RESET}')

        if check:
            self.check()

    @property
    def uuid(self):
        return self._uuid

    @uuid.setter
    def uuid(self, value):
        old_uuid = self._uuid
        self._uuid = value
        self._replace_uuids(old_uuid, value)

    def _replace_uuids(self, old_uuid, value):
        if self.content.c_pages.last_opened.value == old_uuid:
            self.content.c_pages.last_opened.value = value
        for file in self.files:
            file.uuid = file.uuid.replace(old_uuid, value)
        for key in list(self.files_available.keys()):
            file = self.files_available.pop(key)
            key = key.replace(old_uuid, value)
            self.files_available[key] = file
        for key in list(self.content_data.keys()):
            file = self.content_data.pop(key)
            key = key.replace(old_uuid, value)
            self.content_data[key] = file

    @property
    def content_files(self):
        return [file.uuid for file in self.files if
                any(file.uuid.endswith(file_type) for file_type in self.CONTENT_FILE_TYPES)]

    @property
    def file_uuid_map(self):
        return {
            file.uuid: file
            for file in self.files
        }

    @property
    def available(self):
        return all(file in self.files_available.keys() for file in self.content_files)

    def __download_file_task(self, file: File, operation: DownloadOperation, cancel_event: threading.Event):
        """
        Internal method to download a file with a cancel event.
        This is used to ensure that the download can be cancelled.
        """
        try:
            with self.api.download_lock(operation):
                self.content_data[file.uuid] = get_file_contents(self.api, file.hash, binary=True,
                                                                 stage=DOWNLOAD_CONTENT, operation=operation,
                                                                 auto_finish=False)
        except DownloadOperation.DownloadCancelException:
            cancel_event.set()
            raise

    # noinspection PyTypeChecker
    def _download_files(self, callback=None):
        if self.api.offline_mode:
            if callback is not None:
                callback()
            return
        operations: List[DownloadOperation] = []
        files = [file for file in self.files if file.uuid in self.content_files]
        for file in files:
            operations.append(DownloadOperation(file.hash, DOWNLOAD_CONTENT, self))
            operations[-1].total = file.size
            self.add_download_operation(operations[-1])
            self.api.add_download_operation(operations[-1])
        cancel_event = threading.Event()
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.__download_file_task, file, operation, cancel_event)
                for file, operation in zip(files, operations)
            ]
            try:
                for future in concurrent.futures.as_completed(futures):
                    if cancel_event.is_set():
                        break
                    future.result()
            except DownloadOperation.DownloadCancelException:
                for _operation in operations:
                    self.api.cancel_download_operation(_operation)
                self.files_available = {}
                return
        for operation in operations:
            if operation.done >= operation.total or operation.stage == FETCH_FILE:
                self.api.finish_download_operation(operation)
        self.files_available = self.check_files_availability()
        self.check()
        if callback is not None:
            callback()

    def _load_files(self, callback=None):
        for file in self.files:
            if file.uuid not in self.content_files:
                continue
            if file.uuid in self.content_data:
                continue
            try:
                data = get_file_contents(self.api, file.hash, binary=True, update=self, ref=self, enforce_cache=True)
            except CacheMiss:
                self.files_available = self.check_files_availability()
                self.check()
                self._download_files(callback)
                return
            if data:
                self.content_data[file.uuid] = data
        if callback:
            callback()

    def unload_files(self):
        to_unload = []
        for file_uuid, data in self.content_data.items():
            if file_uuid in self.content_files:
                to_unload.append(file_uuid)
        for file_uuid in to_unload:
            del self.content_data[file_uuid]

    def load_files_from_cache(self):
        for file in self.files:
            if file.uuid not in self.content_files:
                continue
            try:
                data = get_file_contents(self.api, file.hash, binary=True, enforce_cache=True, update=self)
            except CacheMiss:
                raise
            if data:
                self.content_data[file.uuid] = data

    def ensure_download_and_callback(self, callback):
        if not self.available:
            threading.Thread(target=self._download_files, args=(callback,)).start()
        else:
            threading.Thread(target=self._load_files, args=(callback,)).start()

    def ensure_download(self):
        if not self.available:
            self._download_files()
        else:
            self._load_files()

    def check_files_availability(self) -> Dict[str, File]:
        if not self.api.sync_file_path:
            return {}
        available = {}
        for file in self.files:
            if file.uuid in self.content_data:  # Check if the file was loaded (could be a new file)
                available[file.uuid] = file
                continue
            if os.path.exists(os.path.join(self.api.sync_file_path, file.hash)):  # Check if the file was cached
                available[file.uuid] = file
                continue
        return available

    def export(self):
        self.content_data[f'{self.uuid}.metadata'] = json.dumps(self.metadata.to_dict(), indent=4).encode()
        self.content_data[f'{self.uuid}.content'] = json.dumps(self.content.to_dict(), indent=4).encode()

        for file in self.files:
            if data := self.content_data.get(file.uuid):
                file.hash = make_hash(data)

    @property
    def parent(self):
        return self.metadata.parent

    @parent.setter
    def parent(self, value):
        self.metadata.parent = value

    def check(self):
        self.content.check(self)

    @classmethod
    def new_notebook(cls, api: 'API', name: str, parent: str = None, document_uuid: str = None, page_count: int = 1,
                     notebook_data: List[Union[bytes, FileHandle]] = [], metadata: Metadata = None,
                     content: Content = None) -> 'Document':
        if not (write_blocks or blank_document):
            raise ImportError('rm_lines is not available, please install rm_lines to use this feature')
        metadata = Metadata.new(name, parent) if not metadata else metadata
        content = Content.new_notebook(api.author_id, page_count) if not content else content

        blank_notebook_buffer = BytesIO()
        write_blocks(blank_notebook_buffer, blank_document(api.author_id))
        blank_notebook = blank_notebook_buffer.getvalue()

        if document_uuid is None:
            document_uuid = make_uuid()

        content_data: List[bytes] = [
            json.dumps(content.to_dict(), indent=4).encode(),
            json.dumps(metadata.to_dict(), indent=4).encode(),
            *notebook_data,
            *[
                blank_notebook
                for _ in range(min(1, page_count - len(notebook_data)))
            ]
        ]

        files = [
            File(make_hash(content_data[0]), f"{document_uuid}.content", 0, len(content_data[0])),
            File(make_hash(content_data[1]), f"{document_uuid}.metadata", 0, len(content_data[1])),
            *[
                File(make_hash(data), f"{document_uuid}/{content.c_pages.pages[i].id}.rm", 0, len(data))
                for i, data in enumerate(content_data[2:], 0)
            ]
        ]

        document = cls(api, content, metadata, files, document_uuid)
        document.content_data = {file.uuid: data for file, data in zip(files, content_data)}
        document.files_available = document.check_files_availability()

        return document

    @classmethod
    def new_pdf(cls, api: 'API', name: str, pdf_data: bytes, parent: str = None, document_uuid: str = None):
        if document_uuid is None:
            document_uuid = make_uuid()
        content = Content.new_pdf()
        metadata = Metadata.new(name, parent)

        content_uuid = f'{document_uuid}.content'
        metadata_uuid = f'{document_uuid}.metadata'
        pdf_uuid = f'{document_uuid}.pdf'

        content_data = {
            content_uuid: json.dumps(content.to_dict(), indent=4),
            metadata_uuid: json.dumps(metadata.to_dict(), indent=4),
            pdf_uuid: pdf_data
        }

        content_hashes = {
            content_uuid: content.hash,
            metadata_uuid: metadata.hash,
            pdf_uuid: make_hash(pdf_data)
        }

        document = cls(api, content, metadata, [
            File(content_hashes[key], key, 0, len(content))
            for key, content in content_data.items()
        ], document_uuid, check=False)

        document.content_data = content_data
        document.files_available = document.check_files_availability()

        return document

    @classmethod
    def new_epub(cls, api: 'API', name: str, epub_data: bytes, parent: str = None, document_uuid: str = None):
        if document_uuid is None:
            document_uuid = make_uuid()
        content = Content.new_epub()
        metadata = Metadata.new(name, parent)

        content_uuid = f'{document_uuid}.content'
        metadata_uuid = f'{document_uuid}.metadata'
        epub_uuid = f'{document_uuid}.epub'

        content_data = {
            content_uuid: json.dumps(content.to_dict(), indent=4),
            metadata_uuid: json.dumps(metadata.to_dict(), indent=4),
            epub_uuid: epub_data
        }

        content_hashes = {
            content_uuid: content.hash,
            metadata_uuid: metadata.hash,
            epub_uuid: make_hash(epub_data)
        }

        document = cls(api, content, metadata, [
            File(content_hashes[key], key, 0, len(content))
            for key, content in content_data.items()
        ], document_uuid)

        document.content_data = content_data
        document.files_available = document.check_files_availability()

        return document

    @classmethod
    def __copy(cls, document: 'Document', shallow: bool = True):
        # Duplicate content and metadata
        metadata = Metadata(document.metadata.to_dict(), document.file_uuid_map[f'{document.uuid}.metadata'].hash)
        content = Content(document.content.to_dict(), metadata, document.file_uuid_map[f'{document.uuid}.content'].hash)

        # Make a new document
        if shallow:
            files = document.files
        else:
            files = [
                copy(file)
                for file in document.files
            ]

        new = cls(document.api, content, metadata, files, document.uuid)
        if shallow:
            new.content_data = copy(document.content_data)
        else:
            new.content_data = deepcopy(document.content_data)
        new.files_available = new.check_files_availability()
        return new

    def __copy__(self):
        return self.__copy(self)

    def __deepcopy__(self, memo=None):
        return self.__copy(self, shallow=False)

    @property
    def __dict__(self):
        return {
            'uuid': self.uuid,
            'server_hash': self.server_hash,
            'content': self.content.__dict__,
            'metadata': self.metadata.__dict__,
            'files_available': list(self.files_available.keys()),
            'files': [file.__dict__ for file in self.files],
            'downloading': self.downloading,
            'provision': self.provision,
            'available': self.available,
        }

    def replace_pdf(self, pdf_data: bytes):
        pdf_uuid = f'{self.uuid}.pdf'
        document = deepcopy(self)

        pdf_file_info = document.file_uuid_map[pdf_uuid]

        pdf_file_info.hash = make_hash(pdf_data)
        pdf_file_info.content_count = len(pdf_data)

        document.content_data[pdf_uuid] = pdf_data

        document.files_available = document.check_files_availability()

        return document

    def get_page_count(self):
        if not self.content.usable:
            return -1
        return len(self.content.c_pages.pages) or -1

    def get_read(self):
        page_count = self.get_page_count()
        if page_count < 0:
            return -1
        return round(((self.metadata.last_opened_page + 1) / max(1, page_count)) * 100) or -1

    def randomize_uuids(self):
        for page in self.content.c_pages.pages:
            old = page.id
            new = make_uuid()
            page.id = new
            self._replace_uuids(old, new)
        self.uuid = make_uuid()

    def duplicate(self):
        new = deepcopy(self)
        new.randomize_uuids()
        new.metadata.last_modified = now_time_int()
        new.metadata.created_time = now_time_int()
        new.provision = True
        return new
