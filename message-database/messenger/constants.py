import os

DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# KAFKA Settings
KAFKA_ENABLED = (os.getenv('KAFKA_ENABLED', 'True') == 'True')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
SM_TOPIC = os.getenv('SM_TOPIC', 'SCHEDULED_MESSAGES')
KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC', 'MESSAGES')
KAFKA_MESSAGE_TIMEOUT = int(os.getenv('KAFKA_MESSAGE_TIMEOUT', '10'))

# Database Settings
DATABASE_ENABLED = (os.getenv('DATABASE_ENABLED', 'False') == 'True')
DATABASE_SERVER = os.getenv('DATABASE_SERVER', 'localhost')

# Message and Experiment Settings
MESSAGE_SIZE_BYTES = int(os.getenv('MESSAGE_SIZE_BYTES', '100'))
MESSAGE_ID_KEY = os.getenv('MESSAGE_ID_KEY', '__sm_msg_id')
EXPERIMENT_ID_KEY = os.getenv('EXPERIMENT_ID_KEY', '__sm_exp_id')
EXPERIMENT_MESSAGE_CREATION_KEY = os.getenv('EXPERIMENT_MESSAGE_CREATION_KEY', '__sm_exp_creation_time')
EXPERIMENT_DURATION_HOURS = float(os.getenv('EXPERIMENT_DURATION_HOURS', '1'))
EXPERIMENT_MESSAGE_COUNT = int(os.getenv('EXPERIMENT_MESSAGE_COUNT', '10000'))

MESSENGER_SCHEDULER_FREQ = int(os.getenv('MESSENGER_SCHEDULER_FREQ', '10'))

COLLECTOR_PROCESS_COUNT = int(os.getenv('COLLECTOR_PROCESS_COUNT', '8'))

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))
REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '1'))
