from enum import Enum

RM_SCREEN_SIZE = (1404, 1872)
RM_SCREEN_CENTER = tuple(v // 2 for v in RM_SCREEN_SIZE)


class ZoomModes(Enum):
    BestFit = 'bestFit'
    CustomFit = 'customFit'
    FitToWidth = 'fitToWidth'
    FitToHeight = 'fitToHeight'
