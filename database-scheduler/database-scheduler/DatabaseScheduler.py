import multiprocessing
import threading
import time
from common import printerror, printsuccess
from constants import *
import datetime
from cassandra.cluster import Cluster

class DatabaseScheduler(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        def __run():
            cluster = Cluster([DATABASE_SCHEDULER_HOST])
            session = cluster.connect()
            session.set_keyspace(DATABASE_SCHEDULER_KEYSPACE.lower())

            cur_time = (datetime.datetime.now() + datetime.timedelta(seconds=DATABASE_SCHEDULER_FREQ)).strftime(TIME_FORMAT)
            msg_headers = [i.replace('__', '') for i in MESSAGES_TABLE_SCHEMA.keys() if i != DATABASE_MESSAGE_RECIEVED_KEY]

            result = session.execute(f"SELECT {', '.join(msg_headers)} FROM {DATABASE_SCHEDULER_SM_TABLE.lower()} WHERE time < '{cur_time}' ALLOW FILTERING")
            query_list = []

            for message in result:
                message_id = message[0]
                insert_keys = msg_headers + [DATABASE_MESSAGE_RECIEVED_KEY.replace('__', '')]
                insert_values = [f"'{str(i)}'" for i in list(message) + [datetime.datetime.now().strftime(TIME_FORMAT)]]

                query_list.append(f"INSERT INTO {DATABASE_SCHEDULER_RECIPIENT_TABLE.lower()} ({', '.join(insert_keys)}) VALUES ({', '.join(insert_values)})")
                query_list.append(f"DELETE FROM {DATABASE_SCHEDULER_SM_TABLE.lower()} WHERE {MESSAGE_ID_KEY.replace('__', '')} = '{message_id}'")
            
            for q in query_list:
                session.execute(q)
            cluster.shutdown()

        while True:
            printsuccess('DatabaseScheduler started')
            try:
                while True:
                    threading.Thread(target=__run).start()
                    time.sleep(DATABASE_SCHEDULER_FREQ)

            except Exception as e:
                printerror(e)
                printerror(f'DatabaseScheduler PID: {threading.get_native_id()} terminated. Restarting...')
                time.sleep(DATABASE_SCHEDULER_FREQ)

if __name__ == '__main__':
    DatabaseScheduler().start()