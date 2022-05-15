import time
from xmlrpc.client import Boolean
from kafka.admin import KafkaAdminClient, NewTopic
from config import DEBUG, KAFKA_SERVER, SM_BUCKET_TOPIC_FORMAT, SM_BUCKETS_MULTIPLICATION_RATIO, SM_MAXIUMUM_DELAY, SM_MINIUMUM_DELAY, SM_TIME_FORMAT, SM_TOPIC, SM_CONSUMER_GROUP_NAME

CACHE = {}

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def getTime():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def printerror(message):
    print(f'{colors.ERROR}[ERROR][{getTime()}] {message}{colors.ENDC}')

def printsuccess(message):
    print(f'{colors.OKGREEN}[OK][{getTime()}] {message}{colors.ENDC}')

def printinfo(message):
    if DEBUG:
        print(f'[INFO][{getTime()}] {message}')

def get_bucket_list():
    
    if 'bucket_list' in CACHE:
        printinfo('Using cached get_bucket_list()')
        return CACHE.bucket_list

    printinfo('Building bucket list')
    
    bucket_list = []
    bucket_object_list = []
    bucket_lower_limit = SM_MINIUMUM_DELAY

    while bucket_lower_limit < SM_MAXIUMUM_DELAY:
        bucket_upper_limit = bucket_lower_limit * SM_BUCKETS_MULTIPLICATION_RATIO
        b_name = SM_BUCKET_TOPIC_FORMAT.replace('$start$', str(bucket_lower_limit)).replace('$end$', str(bucket_upper_limit))
        bucket_list.append(b_name)
        bucket_object_list.append({
            'name' : b_name,
            'lower' : bucket_lower_limit,
            'upper' : bucket_upper_limit
        })

        bucket_lower_limit = bucket_upper_limit
    
    b_name = SM_BUCKET_TOPIC_FORMAT.replace('$start$', str(bucket_lower_limit)).replace('$end$', '0')
    bucket_list.append(b_name)
    bucket_object_list.append({
        'name' : b_name,
        'lower' : bucket_lower_limit,
        'upper' : bucket_upper_limit
    })

    CACHE['bucket_list'] = bucket_list
    CACHE['bucket_object_list'] = bucket_object_list

    return bucket_list

def get_bucket_object_list():
    if 'bucket_object_list' in CACHE:
        printinfo('Using cached get_bucket_object_list()')
    else:
        get_bucket_list()

    return CACHE.bucket_object_list

def __get_admin():
    return KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER, 
    )

def create_topics(topic_list) -> Boolean:
    success = True
    admin_client = __get_admin()

    try:
        topicobject_list = [NewTopic(name=i.name, num_partitions=i.num_partitions, replication_factor=1) for i in topic_list]
        admin_client.create_topics(new_topics=topicobject_list, validate_only=False)
        printsuccess(f'Created topics: {topic_list}')
    except:
        printerror(f'Unable to create topics: {topic_list}')
        success = False
    finally:
        admin_client.close()
    
    return success

def get_config():
    return {
        'kafka_server' : KAFKA_SERVER,
        'bucket_list' : get_bucket_list(),
        'sm_topic' : SM_TOPIC,
        'group' : SM_CONSUMER_GROUP_NAME,
        'time_format' : SM_TIME_FORMAT
    }

if __name__ == "__main__":
    print(get_bucket_list())