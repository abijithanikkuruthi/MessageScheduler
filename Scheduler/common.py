import time
import uuid
from confluent_kafka.admin import AdminClient, NewTopic
from constants import *
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
            printerror(f'[CRITICAL][{self.name}] Lock Timeout. Releasing Lock now. {message}')
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

def printwarning(message):
    print(f'{colors.WARNING}[WARNING][{getTime()}] {message}{colors.ENDC}')
    
def printheader(message):
    print(f'{colors.HEADER}[HEADER] {message}{colors.ENDC}')

def printdebug(message):
    if DEBUG:
        print(f'{colors.OKBLUE}[DEBUG][{getTime()}] {message}{colors.ENDC}')

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
    return AdminClient({'bootstrap.servers': KAFKA_SERVER})

def create_topics(topic_list) -> bool:
    success = True
    admin_client = __get_admin()
    topicobject_list = [ NewTopic(topic = i['name'],
                                    num_partitions = i['num_partitions'],
                                    replication_factor = 1,
                                    config = { 'retention.ms': i['retention'] * 1000,
                                                'delete.retention.ms' : i['retention'] * 1000,
                                                'segment.ms' : i['retention'] * 1000,
                                                'flush.ms' : i['retention'] * 1000,
                                     }) for i in topic_list ]
    t = admin_client.create_topics(topicobject_list)
    for topic, f in t.items():
        try:
            f.result()
        except Exception as e:
            printwarning(f'{e}')
            success = False

    success and printsuccess(f'Created topics: {topic_list}')
    return success

def id_generator():
    return str(uuid.uuid4())

if __name__ == "__main__":
    print(get_bucket_list())