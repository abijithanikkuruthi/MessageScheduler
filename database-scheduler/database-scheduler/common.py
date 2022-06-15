from constants import *
import time
import uuid
import random
import string
from cassandra.cluster import Cluster
import mysql.connector

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

def create_cassandra_keyspace(name):
    # Wait for cassandra to be ready
    while True:
        try:     
            cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
            session = cluster.connect()
            break
        except Exception as e:
            time.sleep(REQUEST_ERROR_WAIT_TIME)
        
    try:
        session.execute(f'CREATE KEYSPACE {name.lower()} WITH replication = {{\'class\': \'SimpleStrategy\', \'replication_factor\': 1}};')
        printsuccess(f"Created keyspace {name.lower()}")
    except Exception as e:
        printwarning(f"Error creating keyspace {name}: {e}")
    finally:
        cluster.shutdown()

def create_cassandra_table(keyspace_name, table_name, table_schema):
    # Wait for cassandra to be ready
    while True:
        try:     
            cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
            session = cluster.connect()
            break
        except Exception as e:
            time.sleep(REQUEST_ERROR_WAIT_TIME)
        
    try:
        session.set_keyspace(keyspace_name.lower())
        query = f'CREATE TABLE {table_name.lower()} ({table_schema});'
        session.execute(query)
        printsuccess(f"Created table {table_name}")
    except Exception as e:
        printwarning(f"Error creating table {table_name}: {e}")
    finally:
        cluster.shutdown()

def get_insert_message_cassandra(message):
    insert_message = ""
    insert_key = ""
    for k, v in MESSAGES_TABLE_SCHEMA_CASSANDRA.items():
        try:
            insert_message += f"'{message['header'][k]}', "
            insert_key += f"{k.replace('__', '')}, "
        except Exception as e:
            continue
    
    insert_message += f"'{message['value']}'"
    insert_key += "value"
    return insert_key, insert_message

def create_mysql_database(database_name):
    global cnx
    try:
        while True:
            try:
                # Wait for MySQL to be ready
                cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER, 
                                        password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                                    host=DATABASE_SCHEDULER_MYSQL_HOST)
                cursor = cnx.cursor()
                break
            except Exception as e:
                time.sleep(REQUEST_ERROR_WAIT_TIME)
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database_name))
        cnx.commit()
        cursor.close()
        cnx.close()
        printsuccess(f'Created database: {database_name}')
    except Exception as e:
        cnx.close()
        printwarning("Failed creating database: {}".format(e))

def create_mysql_table(database_name, table_name, table_schema):
    try:
        while True:
            try:
                # Wait for the database to be ready
                cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER, 
                                        password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                                    host=DATABASE_SCHEDULER_MYSQL_HOST,
                                    database=database_name)
                cursor = cnx.cursor()
                break
            except Exception as e:
                time.sleep(REQUEST_ERROR_WAIT_TIME)
        cursor.execute(f"CREATE TABLE {table_name} ({table_schema})")
        cnx.commit()
        cursor.close()
        printsuccess(f'Created table: {table_name}')
    except Exception as e:
        printwarning("Failed creating table: {}".format(e))

def get_insert_message_mysql(message):
    insert_message = ""
    insert_key = ""
    for k, v in MESSAGES_TABLE_SCHEMA_MYSQL.items():
        try:
            insert_message += f"'{message['header'][k]}', "
            insert_key += f"{k}, "
        except Exception as e:
            continue
    insert_message += f"'{message['value']}'"
    insert_key += "value"
    return insert_key, insert_message

if __name__=="__main__":
    pass