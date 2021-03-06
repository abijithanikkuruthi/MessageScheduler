import os

# Enable debugging for detailed logs
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# KAFKA Settings
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_APPLICATION_RESTART_TIME = 5 * 60 # 5 minutes

# Scheduled Messages Topic Settings
SM_TOPIC = 'SCHEDULED_MESSAGES'
SM_TOPIC_PARTITIONS = int(os.getenv('SM_TOPIC_PARTITIONS', '16'))
SM_CONSUMER_GROUP_PREFIX = '__SM_GROUP_'
SM_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
SM_MH_CONSUMER_GROUP_ID = '__SM_MH_GROUP'
SM_MH_PROCESS_COUNT = 16
SM_HEADER_JOB_ID_KEY = '__sm_job_id'
SM_HEADER_MH_TIMESTAMP_KEY = '__sm_mh_timestamp'
SM_HEADER_WORKER_TIMESTAMP_KEY = '__sm_worker_timestamp'
SM_HEADER_MESSAGE_HOPCOUNT_KEY = '__sm_message_hopcount'

# Scheduled Message Bucketting Configuration
SM_MINIUMUM_DELAY = int(os.getenv('SM_MINIUMUM_DELAY', '60'))
SM_MAXIUMUM_DELAY = int(os.getenv('SM_MAXIUMUM_DELAY', '1500'))
SM_BUCKETS_MULTIPLICATION_RATIO = int(os.getenv('SM_BUCKETS_MULTIPLICATION_RATIO', '3'))
SM_PARTITIONS_PER_BUCKET = int(os.getenv('SM_PARTITIONS_PER_BUCKET', '16'))
SM_BUCKET_TOPIC_FORMAT = '__SM_BUCKET_$start$_$end$'
SM_BUCKET_PROCESS_TIME_FRACTION = 0.0

# Scheduled JOB Settings
class JOB_STATUS_LIST:
    READY = 'READY'
    WORKING = 'WORKING'
    DONE = 'DONE'
    ERROR = 'ERROR'
JOB_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
JOB_QUEUE_MAX_SIZE = 100
JOB_QUEUE_THREAD_LOCK_TIMEOUT = 5
JOB_LOG_FILE = 'jobs_done_log.csv'

# Scheduled Message Worker Settings
class WORKER_STATUS_LIST:
    READY = 'READY'
    WORKING = 'WORKING'
    DONE = 'DONE'
    ERROR = 'ERROR'
WORKER_SCHEDULER_FREQ = 15
WORKER_READY_POLL_FREQ = 5
WORKER_WORKING_POLL_FREQ = 15
WORKER_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WORKER_QUEUE_THREAD_LOCK_TIMEOUT = 5
WORKER_STALE_TIME = 45
WORKER_PROCESS_COUNT = int(os.getenv('WORKER_PROCESS_COUNT', 8))
WORKER_CONSUMER_TIMEOUT = 10

# Configuration Server Settings
SERVER_HOST = '0.0.0.0'
SERVER_PORT = os.getenv('SERVER_PORT', '8000')