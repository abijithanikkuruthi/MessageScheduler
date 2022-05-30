import multiprocessing
import threading
import time
import mysql.connector
from common import printerror, printsuccess
from constants import *
import datetime

class DatabaseScheduler(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        def __run():
            cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_USER, 
                                password=DATABASE_SCHEDULER_PASSWORD,
                                host=DATABASE_SCHEDULER_HOST,
                                database=DATABASE_SCHEDULER_DATABASE)
            cursor = cnx.cursor()
            cur_time = (datetime.datetime.now() + datetime.timedelta(seconds=DATABASE_SCHEDULER_FREQ)).strftime(TIME_FORMAT)
            msg_headers = [i for i in MESSAGES_TABLE_SCHEMA.keys() if i != DATABASE_MESSAGE_RECIEVED_KEY]

            cursor.execute(f"SELECT {', '.join(msg_headers)} FROM {DATABASE_SCHEDULER_SM_TABLE} WHERE `time` < '{cur_time}' AND `delivered` IS NULL")
            query_list = []

            for message in cursor:
                message_id = message[0]
                insert_keys = msg_headers + [DATABASE_MESSAGE_RECIEVED_KEY]
                insert_values = [f"'{str(i)}'" for i in list(message) + [datetime.datetime.now().strftime(TIME_FORMAT)]]

                query_list.append(f"INSERT INTO {DATABASE_SCHEDULER_RECIPIENT_TABLE} ({', '.join(insert_keys)}) VALUES ({', '.join(insert_values)})")
                query_list.append(f"UPDATE {DATABASE_SCHEDULER_SM_TABLE} SET `delivered` = '1' WHERE `{MESSAGE_ID_KEY}` = '{message_id}'")
            
            for q in query_list:
                cursor.execute(q)
                
            cnx.commit()
            cursor.close()
            cnx.close()

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