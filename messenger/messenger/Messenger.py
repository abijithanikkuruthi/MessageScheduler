import multiprocessing
import time
from random import randrange
import datetime
from confluent_kafka import Producer
from cassandra.cluster import Cluster
from pymongo import MongoClient
import mysql.connector

from common import printsuccess, id_generator, printerror, getTime, printwarning, random_data, get_insert_message_cassandra, get_insert_message_mysql
from constants import *

class ProgressInfo:
    def __init__(self, name, total) -> None:
        self.total = total
        self.name = name
        self.current = 0
        self.progress_keys = ['1', '10', '25', '33' ,'50', '67', '75', '90']
        self.progress = { k : False for k in self.progress_keys }
    
    def update(self, current):
        self.current = current
        percent = int(self.current / self.total * 100)
        message = None
        for p in self.progress_keys:
            if percent >= int(p) and not self.progress[p]:
                self.progress[p] = True
                message = p + '%'

        if message:
            printsuccess(f'{self.name} progress: {message}')

class Messenger(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        
        self.job_id = id_generator()
        self.start_time = time.time()
        self.end_time = time.time() + (EXPERIMENT_DURATION_HOURS * 3600)
    
    def run(self):
        def __get_next_message_block(current_message_count):
            def __build_message():
                def __get_random_timedelta():
                    upper_limit = max(self.end_time - time.time(), 2)
                    return datetime.timedelta(seconds=randrange(0, int(upper_limit)))
                time_to_send = datetime.datetime.now() + __get_random_timedelta()
                time_to_send = time_to_send.strftime(TIME_FORMAT)
                return { 'header' : {
                            EXPERIMENT_MESSAGE_CREATION_KEY : getTime(),
                            EXPERIMENT_ID_KEY : self.job_id,
                            MESSAGE_ID_KEY : id_generator(),
                            'topic' : KAFKA_MESSAGE_TOPIC,
                            'time' : time_to_send,
                        },
                        'value' : random_data(MESSAGE_SIZE_BYTES),
                    }
            
            if current_message_count >= EXPERIMENT_MESSAGE_COUNT:
                return []

            time_left = self.end_time - time.time()
            messages_left = EXPERIMENT_MESSAGE_COUNT - current_message_count
            
            if time_left <= MESSENGER_SCHEDULER_FREQ:
                return [__build_message() for i in range(messages_left)]

            message_blocks = time_left / MESSENGER_SCHEDULER_FREQ

            messages_to_send = min(int(messages_left / message_blocks), messages_left)
            return [__build_message() for i in range(messages_to_send)]

        def __kafka_send_messages():
            def __kafka_header(headers):
                return [(k, bytes(v, 'utf-8')) for k, v in headers.items()]

            producer = Producer({ 'bootstrap.servers': KAFKA_SERVER, 'client.id' : self.job_id })
            message_sent = 0
            kafka_progress = ProgressInfo('Kafka', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                message_list = __get_next_message_block(message_sent)

                start_time = time.time()
                for message in message_list:
                    while True:
                        try:
                            producer.produce(topic=SM_TOPIC, value=bytes(message['value'], 'utf-8'), headers=__kafka_header(message['header']))
                            break
                        except Exception as e:
                            time.sleep(0.01)

                producer.flush()
                message_sent += len(message_list)
                kafka_progress.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            printsuccess(f'Kafka messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')

        def __database_scheduler_cassandra_send_messages():
            while True:
                try:
                    cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
                    session = cluster.connect()
                    session.set_keyspace(DATABASE_SCHEDULER_DATABASE)
                    break
                except Exception as e:
                    time.sleep(0.01)

            message_sent = 0
            database_progress_cassandra = ProgressInfo('Cassandra', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                message_list = __get_next_message_block(message_sent)

                start_time = time.time()
                for message in message_list:
                    while True:
                        try:
                            insert_keys, insert_values = get_insert_message_cassandra(message)
                            session.execute(f"INSERT INTO {DATABASE_SCHEDULER_SM_TABLE} ({insert_keys}) VALUES ({insert_values})")
                            break
                        except Exception as e:
                            time.sleep(0.01)
                message_sent += len(message_list)
                database_progress_cassandra.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            cluster.shutdown()
            printsuccess(f'Cassandra messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')

        def __database_scheduler_mysql_send_messages():
            cnx = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER, 
                                password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                              host=DATABASE_SCHEDULER_MYSQL_HOST,
                              database=DATABASE_SCHEDULER_DATABASE)
            cursor = cnx.cursor()

            message_sent = 0
            database_progress_mysql = ProgressInfo('MySql', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                message_list = __get_next_message_block(message_sent)

                start_time = time.time()
                for message in message_list:
                    while True:
                        try:
                            insert_keys, insert_values = get_insert_message_mysql(message)
                            cursor.execute(f"INSERT INTO {DATABASE_SCHEDULER_SM_TABLE} ({insert_keys}) VALUES ({insert_values})")
                            break
                        except Exception as e:
                            time.sleep(0.01)
                cnx.commit()
                message_sent += len(message_list)
                database_progress_mysql.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            cursor.close()
            cnx.close()
            printsuccess(f'MySql messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')

        def __messenger_database_send():
            # This function was written to make sure no messages are being lost by Kafka or Cassandra or Mysql.
            # Multiple experiment runs showed there was no data loss.
            # So currently disabling this function.
            return
            message_database = MongoClient(MESSAGE_DATABASE_URL)[KAFKA_MESSAGE_TOPIC][KAFKA_MESSAGE_TOPIC]

            message_sent = 0
            message_database_progress = ProgressInfo('Message Database', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                ul = __get_next_block_index(message_sent)
                start_time = time.time()
                for message in message_list[message_sent:ul]:
                    while True:
                        try:
                            message['header'][EXPERIMENT_MESSAGE_CREATION_KEY] = getTime()
                            message_database.insert_one(message['header'])
                            break
                        except Exception as e:
                            time.sleep(0.01)
                message_sent = ul
                message_database_progress.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            printsuccess(f'Message Database messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')
        
        printsuccess(f'Preparing to send {EXPERIMENT_MESSAGE_COUNT} messages in {EXPERIMENT_DURATION_HOURS} hours ({int(EXPERIMENT_DURATION_HOURS*60)} minutes)')

        if KAFKA_ENABLED:
            kafka_process = multiprocessing.Process(target=__kafka_send_messages)
            kafka_process.start()
        if DATABASE_SCHEDULER_CASSANDRA_ENABLED:
            database_scheduler_cassandra_process = multiprocessing.Process(target=__database_scheduler_cassandra_send_messages)
            database_scheduler_cassandra_process.start()
        if DATABASE_SCHEDULER_MYSQL_ENABLED:
            database_scheduler_mysql_process = multiprocessing.Process(target=__database_scheduler_mysql_send_messages)
            database_scheduler_mysql_process.start()
        if MESSENGER_DATABASE_ENABLED:
            messenger_database_process = multiprocessing.Process(target=__messenger_database_send)
            messenger_database_process.start()
        
        time.sleep(EXPERIMENT_DURATION_HOURS * 3600)

        experiment_finished = False
        while not experiment_finished:
            experiment_finished = True
            console_message = 'Waiting for processes to finish: '
            if KAFKA_ENABLED and kafka_process.is_alive():
                console_message += 'Kafka '
                experiment_finished = False
            if DATABASE_SCHEDULER_CASSANDRA_ENABLED and database_scheduler_cassandra_process.is_alive():
                console_message += 'Cassandra '
                experiment_finished = False
            if DATABASE_SCHEDULER_MYSQL_ENABLED and database_scheduler_mysql_process.is_alive():
                console_message += 'MySql '
                experiment_finished = False
            if MESSENGER_DATABASE_ENABLED and messenger_database_process.is_alive():
                console_message += 'Message Database '
                experiment_finished = False
            if experiment_finished:
                break
            printwarning(console_message)

            time.sleep(MESSENGER_SCHEDULER_FREQ)
        
        self.finish_time = time.time()
        printsuccess(f'Messenger finished sending {EXPERIMENT_MESSAGE_COUNT} messages in {int((self.finish_time - self.start_time)/(60))} minutes.')


if __name__ == '__main__':
    messenger = Messenger()
    messenger.start()
    messenger.join()