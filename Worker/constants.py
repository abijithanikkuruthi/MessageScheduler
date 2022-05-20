import os

# Enable debugging for detailed logs
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

SCHEDULER_SERVER_URL = os.getenv('SCHEDULER_SERVER_URL', 'http://localhost:8000')
REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '1'))
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))