from constants import DEBUG, REQUEST_ERROR_WAIT_TIME, SCHEDULER_SERVER_URL, REQUEST_COUNT_LIMIT
import requests
import time

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
                printdebug(f'Config updated from {cls.CONFIG_URL}')
                cls.CONFIG['last_update'] = getTime()
                cls.CONFIG['bucket_object_list'] = sorted(cls.CONFIG['bucket_object_list'], key=lambda k: k['lower'])
                printdebug(f'Config: {cls.CONFIG}')
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

def getTime(fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime(fmt)

def printerror(message):
    print(f'{colors.ERROR}[ERROR][{getTime()}] {message}{colors.ENDC}')

def printsuccess(message):
    print(f'{colors.OKGREEN}[OK][{getTime()}] {message}{colors.ENDC}')

def printinfo(message):
    print(f'[INFO][{getTime()}] {message}')

def printheader(message):
    print(f'{colors.HEADER}[HEADER] {message}{colors.ENDC}')

def printwarning(message):
    print(f'{colors.WARNING}[WARNING][{getTime()}] {message}{colors.ENDC}')

def printdebug(message):
    if DEBUG:
        print(f'{colors.OKBLUE}[DEBUG][{getTime()}] {message}{colors.ENDC}')

def send_response_from_url(url, req_type, data):
    req_count = 0
    while req_count < REQUEST_COUNT_LIMIT:
        req_count = req_count + 1
        try:
            printdebug(f'[{req_type}] {url}')
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

if __name__=="__main__":
    print(Config.get())