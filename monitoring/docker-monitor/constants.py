import os

# Enable debugging for detailed logs
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

KAFKA_ENABLED = (os.getenv('KAFKA_ENABLED', 'True') == 'True')
KAFKA_SCHEDULER_SERVER_URL = os.getenv('KAFKA_SCHEDULER_SERVER_URL', 'http://localhost:8000')

DATABASE_SCHEDULER_CASSANDRA_ENABLED = (os.getenv('DATABASE_SCHEDULER_CASSANDRA_ENABLED', 'True') == 'True')
DATABASE_SCHEDULER_MYSQL_ENABLED = (os.getenv('DATABASE_SCHEDULER_MYSQL_ENABLED', 'True') == 'True')

REQUEST_ERROR_WAIT_TIME = int(os.getenv('REQUEST_ERROR_WAIT_TIME', '2'))
REQUEST_COUNT_LIMIT = int(os.getenv('REQUEST_COUNT_LIMIT', '3'))
DOCKER_MONITOR_INTERVAL = int(os.getenv('DOCKER_MONITOR_INTERVAL', '30'))

LOGS_FOLDER = 'logs'

DOCKER_MONITOR_LOG_FILE = 'docker.log'
KAFKA_SCHEDULER_JOB_LOG_FILE = 'kafka-scheduler-job.log'
KAFKA_SCHEDULER_CONFIG_FILE = 'kafka-scheduler-config.json'