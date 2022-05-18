import time
import random
import string
from kafka.admin import KafkaAdminClient, NewTopic
from config import DEBUG, KAFKA_SERVER, SM_BUCKET_TOPIC_FORMAT, SM_BUCKETS_MULTIPLICATION_RATIO, SM_MAXIUMUM_DELAY, SM_MINIUMUM_DELAY, SM_TIME_FORMAT, SM_TOPIC, SM_CONSUMER_GROUP_NAME
import threading
from contextlib import contextmanager

CACHE = {}

class TimeoutLock(object):
    def __init__(self, lock_name: str):
        self._lock = threading.Lock()
        self.name = lock_name

    def acquire(self, blocking=True, timeout=-1):
        return self._lock.acquire(blocking, timeout)

    @contextmanager
    def acquire_timeout(self, timeout: int, message: str):
        result = self._lock.acquire(timeout=timeout)
        yield result
        if not result:
            printerror(f'[CRITICAL][{self.lock_name}] Lock Timeout. Releasing Lock now. {message}')
        self._lock.release()

    def release(self):
        self._lock.release()

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

def getTime(fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime(fmt)

def printerror(message):
    print(f'{colors.ERROR}[ERROR][{getTime()}] {message}{colors.ENDC}')

def printsuccess(message):
    print(f'{colors.OKGREEN}[OK][{getTime()}] {message}{colors.ENDC}')

def printinfo(message):
    print(f'[INFO][{getTime()}] {message}')

def printdebug(message):
    if DEBUG:
        print(f'[DEBUG][{getTime()}] {message}')

def get_bucket_list():
    
    if CACHE and CACHE.get('bucket_list'):
        return CACHE['bucket_list']

    printdebug('Building bucket list')
    
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
    if CACHE and CACHE.get('bucket_object_list'):
        return CACHE['bucket_object_list']
    else:
        get_bucket_list()

    return CACHE['bucket_object_list']

def __get_admin():
    return KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER, 
    )

def create_topics(topic_list) -> bool:
    success = True
    admin_client = __get_admin()
    try:
        topicobject_list = [NewTopic(name=i['name'], num_partitions=i['num_partitions'], replication_factor=1) for i in topic_list]
        admin_client.create_topics(new_topics=topicobject_list, validate_only=False)
        printsuccess(f'Created topics: {topic_list}')
    except Exception as e:
        printerror(f'Unable to create topics: {topic_list}')
        success = False
    finally:
        admin_client.close()
    
    return success

def get_config():
    if CACHE and CACHE.get('config'):
        return CACHE['config']

    cfg_obj = {
        'kafka_server' : KAFKA_SERVER,
        'bucket_list' : get_bucket_list(),
        'bucket_object_list' : get_bucket_object_list(),
        'sm_topic' : SM_TOPIC,
        'group' : SM_CONSUMER_GROUP_NAME,
        'sm_time_format' : SM_TIME_FORMAT,
    }

    CACHE['config'] = cfg_obj
    return cfg_obj

def id_generator(size=24, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

if __name__ == "__main__":
    print(get_bucket_list())