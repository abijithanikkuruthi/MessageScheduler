from constants import *
import time
import uuid
import random
import string
from datetime import datetime, timedelta, date
from cassandra.cluster import Cluster

CACHE = {}
cnx = None

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

def create_keyspace(name):
    while True:
        try:     
            cluster = Cluster([DATABASE_SCHEDULER_HOST])
            session = cluster.connect()
            break
        except Exception as e:
            printwarning(f"Error connecting to database: {e}\nRetrying...")
        
    try:
        session.execute(f'CREATE KEYSPACE {name.lower()} WITH replication = {{\'class\': \'SimpleStrategy\', \'replication_factor\': 1}};')
        cluster.shutdown()
        printsuccess(f"Created keyspace {name.lower()}")
        return True
    except Exception as e:
        printwarning(f"Error creating keyspace {name}: {e}")
        cluster.shutdown()

def create_table(keyspace_name, table_name, table_schema):
    while True:
        try:     
            cluster = Cluster([DATABASE_SCHEDULER_HOST])
            session = cluster.connect()
            break
        except Exception as e:
            printwarning(f"Error connecting to database: {e}\nRetrying...")
        
    try:
        cluster = Cluster([DATABASE_SCHEDULER_HOST])
        session = cluster.connect()
        session.set_keyspace(keyspace_name.lower())
        query = f'CREATE TABLE {table_name.lower()} ({table_schema});'
        session.execute(query)
        cluster.shutdown()
        printsuccess(f"Created table {table_name}")
        return True
    except Exception as e:
        printwarning(f"Error creating table {table_name}: {e}")
        cluster.shutdown()

def get_insert_message(message):
    insert_message = ""
    insert_key = ""
    for k, v in MESSAGES_TABLE_SCHEMA.items():
        try:
            insert_message += f"'{message[k]}', "
            insert_key += f"{k}, "
        except Exception as e:
            continue
    insert_message = insert_message[:-2]
    insert_key = insert_key[:-2]
    return insert_key, insert_message

if __name__=="__main__":
    pass