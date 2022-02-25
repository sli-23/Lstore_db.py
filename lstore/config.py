# Global Setting
# Static Global Variables

# Page size: 4096b
PAGE_SIZE = 4096
RECORDS_PER_PAGE = PAGE_SIZE / 8
MAXPAGE = 16
MAXINT = 2**64 - 1
BUFFERPOOL_SIZE = 100 #the default buffer pool will be 1,000 pages or 4 MB

# Merge
MERGE_TRIGGRE = RECORDS_PER_PAGE * 3 #EVERY 3 * 512 record to merge tail page

# Column 
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
DEFAULT_COLUMN = 4

