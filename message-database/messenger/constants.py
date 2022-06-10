import os

DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# KAFKA Settings
KAFKA_ENABLED = (os.getenv('KAFKA_ENABLED', 'True') == 'True')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
SM_TOPIC = os.getenv('SM_TOPIC', 'SCHEDULED_MESSAGES')
KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC', 'MESSAGES')
KAFKA_MESSAGE_TOPIC_PARTITIONS = int(os.getenv('KAFKA_MESSAGE_TOPIC_PARTITIONS', '16'))
KAFKA_MESSAGE_TIMEOUT = int(os.getenv('KAFKA_MESSAGE_TIMEOUT', '10'))
KAFKA_MESSAGE_GROUP_ID = os.getenv('KAFKA_MESSAGE_GROUP_ID', 'MESSAGE_GROUP')

# Database Settings
MESSAGE_DATABASE_ENABLED = (os.getenv('MESSAGE_DATABASE_ENABLED', 'True') == 'True')
MESSAGE_DATABASE_URL = os.getenv('MESSAGE_DATABASE_URL', 'mongodb://admin:kafka@localhost:27017/')
MESSAGE_DATABASE_TABLE = os.getenv('MESSAGE_DATABASE_TABLE', 'MESSAGES_RECIEVED')

# Message and Experiment Settings
MESSAGE_SIZE_BYTES = int(os.getenv('MESSAGE_SIZE_BYTES', '100'))
MESSAGE_ID_KEY = os.getenv('MESSAGE_ID_KEY', '__sm_msg_id')
EXPERIMENT_ID_KEY = os.getenv('EXPERIMENT_ID_KEY', '__sm_exp_id')
SM_HEADER_JOB_ID_KEY = '__sm_job_id'
SM_HEADER_MH_TIMESTAMP_KEY = '__sm_mh_timestamp'
SM_HEADER_WORKER_TIMESTAMP_KEY = '__sm_worker_timestamp'
SM_HEADER_MESSAGE_HOPCOUNT_KEY = '__sm_message_hopcount'
EXPERIMENT_MESSAGE_CREATION_KEY = os.getenv('EXPERIMENT_MESSAGE_CREATION_KEY', '__sm_exp_creation_time')
EXPERIMENT_DURATION_HOURS = float(os.getenv('EXPERIMENT_DURATION_HOURS', '1'))
EXPERIMENT_MESSAGE_COUNT = int(os.getenv('EXPERIMENT_MESSAGE_COUNT', '10000'))

MESSENGER_SCHEDULER_FREQ = int(os.getenv('MESSENGER_SCHEDULER_FREQ', '5'))

COLLECTOR_PROCESS_COUNT = int(os.getenv('COLLECTOR_PROCESS_COUNT', '8'))
COLLECTOR_CONSUMER_TIMEOUT = int(os.getenv('COLLECTOR_CONSUMER_TIMEOUT', '15'))

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))
REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '1'))

# Database Scheduler Settings
DATABASE_SCHEDULER_ENABLED = (os.getenv('DATABASE_SCHEDULER_ENABLED', 'True') == 'True')
DATABASE_SCHEDULER_USER = os.getenv('DATABASE_SCHEDULER_USER', 'root')
DATABASE_SCHEDULER_PASSWORD = os.getenv('DATABASE_SCHEDULER_PASSWORD', 'kafka')
DATABASE_SCHEDULER_HOST = os.getenv('DATABASE_SCHEDULER_HOST', 'localhost')
DATABASE_SCHEDULER_KEYSPACE = os.getenv('DATABASE_SCHEDULER_KEYSPACE', 'MESSAGES')
DATABASE_SCHEDULER_SM_TABLE = os.getenv('DATABASE_SCHEDULER_SM_TABLE', 'MESSAGES')


MESSAGES_TABLE_SCHEMA = {
    MESSAGE_ID_KEY :                    'text',
    'topic':                            'text',
    'time' :                            'timestamp',
    'value':                            'text',
    EXPERIMENT_ID_KEY :                 'text',
    EXPERIMENT_MESSAGE_CREATION_KEY :   'text',
}