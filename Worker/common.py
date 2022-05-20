from constants import DEBUG, REQUEST_ERROR_WAIT_TIME, SCHEDULER_SERVER_URL, REQUEST_COUNT_LIMIT
import requests
import time
import random
import string
import uuid

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

class Config:
    CONFIG = {}
    CONFIG_URL = SCHEDULER_SERVER_URL + '/config'

    def __init__(self):
        if not Config.CONFIG:
            Config.update()
        return Config.CONFIG
    
    @classmethod
    def update(cls):
        while True:
            try:
                cls.CONFIG = get_json_from_url(cls.CONFIG_URL)
                printsuccess(f'Config updated from {cls.CONFIG_URL}')
                cls.CONFIG['last_update'] = getTime()
                cls.CONFIG['bucket_object_list'] = sorted(cls.CONFIG['bucket_object_list'], key=lambda k: k['lower'])
                printinfo(f'Config: {cls.CONFIG}')
                break
            except Exception as e:
                printerror(f'Failed to get config from {Config.CONFIG_URL}: {e}')
                time.sleep(REQUEST_ERROR_WAIT_TIME)
    
    @classmethod
    def get(cls, key=None):
        if not cls.CONFIG:
            cls.update()
        if key:
            return cls.CONFIG[key]
        return cls.CONFIG

class WorkerHandler:
    worker_id = str(uuid.uuid4())
    worker_url = SCHEDULER_SERVER_URL + '/worker/' + worker_id
    work = None

    def __init__(self) -> None:
        pass

    @classmethod
    def get_worker(cls):
        while True:
            try:
                cls.work = get_json_from_url(cls.worker_url)
                printdebug(f'Worker: {cls.work}')
                break
            except Exception as e:
                printerror(f'Failed to get worker from {cls.worker_url}: {e}')
                time.sleep(REQUEST_ERROR_WAIT_TIME)

        return cls.work

    @classmethod
    def post_worker(cls, worker):
        while True:
            try:
                cls.work = post_json_from_url(cls.worker_url, worker)
                printsuccess(f'Worker updated from {cls.worker_url}')
                printdebug(f'Worker: {cls.work}')
                break
            except Exception as e:
                printerror(f'Failed to post worker to {cls.worker_url}: {e}')
                time.sleep(REQUEST_ERROR_WAIT_TIME)
        
        return cls.work

def getTime():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def printerror(message):
    print(f'{colors.ERROR}[ERROR][{getTime()}] {message}{colors.ENDC}')

def printsuccess(message):
    print(f'{colors.OKGREEN}[OK][{getTime()}] {message}{colors.ENDC}')

def printinfo(message):
    print(f'[INFO][{getTime()}] {message}')

def printheader(message):
    print(f'{colors.HEADER}[HEADER] {message}{colors.ENDC}')

def printdebug(message):
    if DEBUG:
        print(f'{colors.OKBLUE}[DEBUG][{getTime()}] {message}{colors.ENDC}')

def send_response_from_url(url, req_type, data):
    req_count = 0
    while req_count < REQUEST_COUNT_LIMIT:
        req_count = req_count + 1
        try:
            printdebug(f'[{req_type}] {url} - {data}')
            if req_type=='GET':
                r = requests.get(url)
            elif req_type=='POST':
                r = requests.post(url, json=data)
            return r
        except:
            printerror(f'Unable to connect to {url}. Retrying...')
            time.sleep(REQUEST_ERROR_WAIT_TIME)
    return False

def get_response_from_url(url):
    return send_response_from_url(url, 'GET', {})

def get_json_from_url(url):
    response = get_response_from_url(url)
    return response.json()

def post_response_from_url(url, data):
    return send_response_from_url(url, 'POST', data)

def post_json_from_url(url, data):
    response = post_response_from_url(url, data)
    return response.json()

def id_generator(size=24, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

if __name__=="__main__":
    print(Config.get())