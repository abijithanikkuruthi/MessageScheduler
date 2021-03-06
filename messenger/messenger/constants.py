import os

DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# Messenger Database Settings
MESSENGER_DATABASE_ENABLED = (os.getenv('MESSAGE_DATABASE_ENABLED', 'True') == 'True')
MESSENGER_DATABASE_URL = os.getenv('MESSENGER_DATABASE_URL', 'mongodb://admin:kafka@localhost:27017/')
MESSENGER_DATABASE_TABLE = os.getenv('MESSAGE_DATABASE_TABLE', 'MESSAGES_RECIEVED')

MESSENGER_SCHEDULER_FREQ = int(os.getenv('MESSENGER_SCHEDULER_FREQ', '60'))

# KAFKA Settings
KAFKA_ENABLED = (os.getenv('KAFKA_ENABLED', 'True') == 'True')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
SM_TOPIC = os.getenv('SM_TOPIC', 'SCHEDULED_MESSAGES')
KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC', 'MESSAGES')
KAFKA_MESSAGE_TOPIC_PARTITIONS = int(os.getenv('KAFKA_MESSAGE_TOPIC_PARTITIONS', '16'))
KAFKA_MESSAGE_TIMEOUT = int(os.getenv('KAFKA_MESSAGE_TIMEOUT', '10'))
KAFKA_MESSAGE_GROUP_ID = os.getenv('KAFKA_MESSAGE_GROUP_ID', 'MESSAGE_GROUP')

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

COLLECTOR_PROCESS_COUNT = int(os.getenv('COLLECTOR_PROCESS_COUNT', '8'))
COLLECTOR_CONSUMER_TIMEOUT = int(os.getenv('COLLECTOR_CONSUMER_TIMEOUT', '10'))
COLLECTOR_CONSUMER_FREQ = int(os.getenv('COLLECTOR_CONSUMER_FREQ', '60'))

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))
REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '1'))

# Database Scheduler Settings
DATABASE_SCHEDULER_DATABASE = os.getenv('DATABASE_SCHEDULER_DATABASE', 'messages')
DATABASE_SCHEDULER_SM_TABLE = os.getenv('DATABASE_SCHEDULER_SM_TABLE', 'messages')

DATABASE_SCHEDULER_CASSANDRA_ENABLED = (os.getenv('DATABASE_SCHEDULER_CASSANDRA_ENABLED', 'True') == 'True')
DATABASE_SCHEDULER_CASSANDRA_HOST = os.getenv('DATABASE_SCHEDULER_CASSANDRA_HOST', 'localhost')

DATABASE_SCHEDULER_MYSQL_ENABLED = (os.getenv('DATABASE_SCHEDULER_MYSQL_ENABLED', 'True') == 'True')
DATABASE_SCHEDULER_MYSQL_HOST = os.getenv('DATABASE_SCHEDULER_MYSQL_HOST', 'localhost')
DATABASE_SCHEDULER_MYSQL_USER = os.getenv('DATABASE_SCHEDULER_MYSQL_USER', 'root')
DATABASE_SCHEDULER_MYSQL_PASSWORD = os.getenv('DATABASE_SCHEDULER_MYSQL_PASSWORD', 'kafka')

# Table Schema
MESSAGES_TABLE_SCHEMA_CASSANDRA = {
    MESSAGE_ID_KEY :                    'text',
    'topic':                            'text',
    'time' :                            'timestamp',
    'value':                            'text',
    EXPERIMENT_ID_KEY :                 'text',
    EXPERIMENT_MESSAGE_CREATION_KEY :   'text',
}

MESSAGES_TABLE_SCHEMA_MYSQL = {
    MESSAGE_ID_KEY :                    'VARCHAR(50)',
    'topic':                            'VARCHAR(50)',
    'time' :                            'DATETIME',
    'value':                          f'VARCHAR({MESSAGE_SIZE_BYTES})',
    EXPERIMENT_ID_KEY :                 'VARCHAR(50)',
    EXPERIMENT_MESSAGE_CREATION_KEY :   'VARCHAR(50)',
}