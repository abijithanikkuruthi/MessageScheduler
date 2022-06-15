import multiprocessing
import time
from common import printerror, printsuccess
from constants import *
import datetime
from cassandra.cluster import Cluster
import mysql.connector

class CassandraDatabaseScheduler(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        def __run(session):
            cur_time = (datetime.datetime.now() + datetime.timedelta(seconds=DATABASE_SCHEDULER_FREQ)).strftime(TIME_FORMAT)
            msg_headers = [i.replace('__', '') for i in MESSAGES_TABLE_SCHEMA_CASSANDRA.keys() if i != DATABASE_MESSAGE_RECIEVED_KEY]

            result = session.execute(f"SELECT {', '.join(msg_headers)} FROM {DATABASE_SCHEDULER_SM_TABLE} WHERE time < '{cur_time}' ALLOW FILTERING")
            query_list = []

            for message in result:
                message_id = message[0]
                insert_keys = msg_headers + [DATABASE_MESSAGE_RECIEVED_KEY.replace('__', '')]
                insert_values = [f"'{str(i)}'" for i in list(message) + [datetime.datetime.now().strftime(TIME_FORMAT)]]

                query_list.append(f"INSERT INTO {DATABASE_SCHEDULER_RECIPIENT_TABLE} ({', '.join(insert_keys)}) VALUES ({', '.join(insert_values)})")
                query_list.append(f"DELETE FROM {DATABASE_SCHEDULER_SM_TABLE} WHERE {MESSAGE_ID_KEY.replace('__', '')} = '{message_id}'")
            
            for q in query_list:
                session.execute(q)
            
        while True:
            try:
                printsuccess("Starting Cassandra database scheduler")
                printsuccess(f"Connecting to {DATABASE_SCHEDULER_CASSANDRA_HOST}")
                cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
                session = cluster.connect()
                session.set_keyspace(DATABASE_SCHEDULER_DATABASE)

                printsuccess(f"Connected to {DATABASE_SCHEDULER_CASSANDRA_HOST}")

                while True:
                    start_time = time.time()
                    __run(session)
                    execution_time = time.time() - start_time
                    time.sleep(max(0, DATABASE_SCHEDULER_FREQ - execution_time))

            except Exception as e:
                printerror(f"Error in Cassandra database scheduler: {e}")
                time.sleep(REQUEST_ERROR_WAIT_TIME)
            finally:
                try:
                    cluster.shutdown()
                except Exception as e:
                    printerror(f"Error shutting down Cassandra database scheduler: {e}")
                    printerror(f'Trying to restart the scheduler')
                    time.sleep(REQUEST_ERROR_WAIT_TIME)

class MySQLDatabaseScheduler(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        def __run(cursor):
            cur_time = (datetime.datetime.now() + datetime.timedelta(seconds=DATABASE_SCHEDULER_FREQ)).strftime(TIME_FORMAT)
            msg_headers = [i for i in MESSAGES_TABLE_SCHEMA_MYSQL.keys() if i != DATABASE_MESSAGE_RECIEVED_KEY]

            cursor.execute(f"SELECT {', '.join(msg_headers)} FROM {DATABASE_SCHEDULER_SM_TABLE} WHERE `time` < '{cur_time}'")
            query_list = []

            for message in cursor:
                message_id = message[0]
                insert_keys = msg_headers + [DATABASE_MESSAGE_RECIEVED_KEY]
                insert_values = [f"'{str(i)}'" for i in list(message) + [datetime.datetime.now().strftime(TIME_FORMAT)]]

                query_list.append(f"INSERT INTO {DATABASE_SCHEDULER_RECIPIENT_TABLE} ({', '.join(insert_keys)}) VALUES ({', '.join(insert_values)})")
                query_list.append(f"DELETE FROM {DATABASE_SCHEDULER_SM_TABLE} WHERE `{MESSAGE_ID_KEY}` = '{message_id}'")

            for q in query_list:
                cursor.execute(q)

            cnx.commit()

        while True:
            try:
                printsuccess("Starting MySQL database scheduler")
                printsuccess(f"Connecting to {DATABASE_SCHEDULER_MYSQL_HOST}")

                cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER, 
                                password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                                host=DATABASE_SCHEDULER_MYSQL_HOST,
                                database=DATABASE_SCHEDULER_DATABASE)
                cursor = cnx.cursor()
            
                printsuccess(f"Connected to {DATABASE_SCHEDULER_MYSQL_HOST}")

                while True:
                    start_time = time.time()
                    __run(cursor)
                    execution_time = time.time() - start_time
                    time.sleep(max(0, DATABASE_SCHEDULER_FREQ - execution_time))

            except Exception as e:
                printerror(f"Error in MySQL database scheduler: {e}")
                time.sleep(REQUEST_ERROR_WAIT_TIME)
            finally:
                try:
                    cnx.close()
                except Exception as e:
                    printerror(f"Error closing MySQL database scheduler: {e}")
                    printerror(f'Trying to restart the scheduler')

if __name__ == '__main__':
    pass