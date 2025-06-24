# Initial stages
STAGE_START = 0
STAGE_GET_ROOT = 1

# Normal upload stages
STAGE_EXPORT_DOCUMENTS = 2
STAGE_DIFF_CHECK_DOCUMENTS = 3
STAGE_PREPARE_DATA = 4
STAGE_COMPILE_DATA = 5
STAGE_PREPARE_ROOT = 6
STAGE_PREPARE_OPERATIONS = 7
STAGE_UPLOAD = 8

# Final stage
STAGE_UPDATE_ROOT = 9

# General sync stage
STAGE_SYNC = 10

# Download operations
UNKNOWN_DOWNLOAD_OPERATION = 11
FETCH_FILE = 12  # Getting the existence of a file
GET_FILE = 13  # Getting the contents of a listing file
GET_CONTENTS = 14  # Getting the contents of any file
DOWNLOAD_CONTENT = 15  # When a file is being downloaded on a user level, not metadata level