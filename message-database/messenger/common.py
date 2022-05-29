from constants import DEBUG, REQUEST_COUNT_LIMIT, REQUEST_ERROR_WAIT_TIME
import requests
import time
import uuid
import random
import string
    
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

def send_response_from_url(url, req_type, data):
    req_count = 0
    while req_count < REQUEST_COUNT_LIMIT:
        req_count = req_count + 1
        try:
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

def id_generator():
    return str(uuid.uuid4())

def random_data(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def excpetion_info(e=None):
    import sys

    exception_type, exception_object, exception_traceback = sys.exc_info()
    filename = exception_traceback.tb_frame.f_code.co_filename
    line_number = exception_traceback.tb_lineno

    printerror(f"Exception type: {exception_type}")
    printerror(f"File name: {filename}")
    printerror(f"Line number: {line_number}")

def save_url_to_file(url, filename):
    response = get_response_from_url(url)
    with open(filename, 'wb') as f:
        f.write(response.content)

if __name__=="__main__":
    pass