import os

DEBUG = (os.getenv('DEBUG', 'False') == 'True')

MESSAGE_ID_KEY = os.getenv('MESSAGE_ID_KEY', '__sm_msg_id')
EXPERIMENT_ID_KEY = os.getenv('EXPERIMENT_ID_KEY', '__sm_exp_id')
EXPERIMENT_MESSAGE_CREATION_KEY = os.getenv('EXPERIMENT_MESSAGE_CREATION_KEY', '__sm_exp_creation_time')
DATABASE_MESSAGE_RECIEVED_KEY = os.getenv('DATABASE_MESSAGE_RECIEVED_KEY', '__sm_recieved_time')
MESSAGE_SIZE_BYTES = int(os.getenv('MESSAGE_SIZE_BYTES', '100'))

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))
REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '1'))

# Database Scheduler Settings
DATABASE_SCHEDULER_HOST = os.getenv('DATABASE_SCHEDULER_HOST', 'localhost')
DATABASE_SCHEDULER_KEYSPACE = os.getenv('DATABASE_SCHEDULER_KEYSPACE', 'MESSAGES')
DATABASE_SCHEDULER_SM_TABLE = os.getenv('DATABASE_SCHEDULER_SM_TABLE', 'MESSAGES')
DATABASE_SCHEDULER_RECIPIENT_TABLE = os.getenv('DATABASE_SCHEDULER_RECIPIENT_TABLE', 'MESSAGES_RECIEVED')
DATABASE_SCHEDULER_FREQ = int(os.getenv('DATABASE_SCHEDULER_FREQ', '60'))

# Build table schema
MESSAGES_TABLE_SCHEMA = {
    MESSAGE_ID_KEY :                    'text',
    'topic':                            'text',
    'time' :                            'timestamp',
    'value':                            'text',
    EXPERIMENT_ID_KEY :                 'text',
    EXPERIMENT_MESSAGE_CREATION_KEY :   'text',
    DATABASE_MESSAGE_RECIEVED_KEY :     'text'
}