import os

DATABASE_SCHEDULER_ENABLED = (os.getenv('DATABASE_SCHEDULER_ENABLED', 'True') == 'True')
MESSAGE_DATABASE_ENABLED = (os.getenv('MESSAGE_DATABASE_ENABLED', 'True') == 'True')
KAFKA_ENABLED = (os.getenv('KAFKA_ENABLED', 'True') == 'True')