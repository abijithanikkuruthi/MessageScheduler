import time
import os
from constants import *

class ProgressInfo:
    def __init__(self, name, total) -> None:
        self.total = int(total)
        self.name = name
        self.current = 0
        self.progress_keys = ['1', '10', '25', '33' ,'50', '67', '75', '90', '100']
        self.progress = { k : False for k in self.progress_keys }
    
    def update(self, current):
        self.current = int(current)
        percent = int(self.current / self.total * 100)
        message = None
        for p in self.progress_keys:
            if percent >= int(p) and not self.progress[p]:
                self.progress[p] = True
                message = p + '%'

        if message:
            printinfo(f'{self.name} progress: {message}')

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

def get_time(fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime(fmt)

def printerror(message):
    print(f'{colors.ERROR}[ERROR][{get_time()}] {message}{colors.ENDC}')

def printsuccess(message):
    print(f'{colors.OKGREEN}[OK][{get_time()}] {message}{colors.ENDC}')

def printinfo(message):
    print(f'[INFO][{get_time()}] {message}')

def printwarning(message):
    print(f'{colors.WARNING}[WARNING][{get_time()}] {message}{colors.ENDC}')
    
def printheader(message):
    print(f'{colors.HEADER}[HEADER] {message}{colors.ENDC}')

def printdebug(message):
    print(f'{colors.OKBLUE}[DEBUG][{get_time()}] {message}{colors.ENDC}')

def get_absolute_path(path):
    return os.path.abspath(path)

def get_config():
    
    config = {
        'data_path': 'data/$timestamp$',
    }
    config['data_path'] = config['data_path'].replace('$timestamp$', get_time("%Y-%m-%d %H-%M-%S"))
    config['data_path'] = get_absolute_path('../../' + config['data_path'])
    os.makedirs(config['data_path'], exist_ok=True)

    config['monitoring'] = {}
    config['monitoring']['docker'] = get_absolute_path('../../monitoring/docker-monitor/logs')
    config['monitoring']['prometheus'] = get_absolute_path('../../monitoring/prometheus')

    return config

if __name__ == '__main__':
    print(get_config())