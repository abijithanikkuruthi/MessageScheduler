from constants import *
import time
import uuid
import random
import string
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta, date

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

def __get_cnx():
    global cnx
    try:
        if not cnx:
            cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_USER, 
                                password=DATABASE_SCHEDULER_PASSWORD,
                              host=DATABASE_SCHEDULER_HOST)
        return cnx
    except Exception as e:
        printwarning(e)
        return False

def create_database(database_name):
    global cnx
    try:
        cnx = __get_cnx()
        cursor = cnx.cursor()
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database_name))
        cnx.commit()
        cursor.close()
        cnx.close()
        cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_USER, 
                                password=DATABASE_SCHEDULER_PASSWORD,
                              host=DATABASE_SCHEDULER_HOST,
                              database=database_name)
        printsuccess(f'Created database: {database_name}')
    except Exception as e:
        cnx.close()
        cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_USER, 
                                password=DATABASE_SCHEDULER_PASSWORD,
                              host=DATABASE_SCHEDULER_HOST,
                              database=database_name)
        printwarning("Failed creating database: {}".format(e))

def create_table(table_name, table_schema):
    try:
        cnx = __get_cnx()
        cursor = cnx.cursor()
        cursor.execute(f"CREATE TABLE {table_name} ({table_schema})")
        cnx.commit()
        cursor.close()
        printsuccess(f'Created table: {table_name}')
    except Exception as e:
        printwarning("Failed creating table: {}".format(e))

def insert_data(table_name, data):
    try:
        cnx = __get_cnx()
        cursor = cnx.cursor()
        cursor.execute(f"INSERT INTO {table_name} VALUES ({data})")
        cnx.commit()
        cursor.close()
    except Exception as e:
        printwarning("Failed inserting data: {}".format(e))

def insert_message(table_name, message):
    insert_data(table_name, get_insert_message(message))

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