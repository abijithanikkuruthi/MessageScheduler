import os

# Enable debugging for detailed logs
DEBUG = (os.getenv('DEBUG', 'False') == 'True')

# KAFKA Settings
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')

# Scheduled Messages Topic Settings
SM_TOPIC = 'SCHEDULED_MESSAGES'
SM_TOPIC_PARTITIONS = 50
SM_CONSUMER_GROUP_NAME = '__SM_GROUP'
SM_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Bucketting Configuration
SM_MINIUMUM_DELAY = 5
SM_MAXIUMUM_DELAY = 80
SM_BUCKETS_MULTIPLICATION_RATIO = 2
SM_PARTITIONS_PER_BUCKET = 50
SM_BUCKET_TOPIC_FORMAT = '__SM_BUCKET_$start$_$end$'

# Configuration Server Settings
CS_HOST = os.getenv('CS_HOST', '0.0.0.0')
CS_PORT = os.getenv('CS_PORT', '8000')