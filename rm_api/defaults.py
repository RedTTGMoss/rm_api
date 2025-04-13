from enum import Enum

RM_SCREEN_SIZE = (1404, 1872)
RM_SCREEN_CENTER = tuple(v // 2 for v in RM_SCREEN_SIZE)


class ZoomModes(Enum):
    BestFit = 'bestFit'  # Default
    CustomFit = 'customFit'
    FitToWidth = 'fitToWidth'
    FitToHeight = 'fitToHeight'


class FileTypes(Enum):
    PDF = 'pdf'
    EPUB = 'epub'
    Notebook = 'notebook'


class Orientations(Enum):
    Portrait = 'portrait'
    Landscape = 'landscape'


class DocumentTypes(Enum):
    Document = 'DocumentType'
    Collection = 'CollectionType'
