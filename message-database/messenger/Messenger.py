import multiprocessing
import time
from random import randrange
import datetime
from confluent_kafka import Producer
from cassandra.cluster import Cluster
from pymongo import MongoClient

from common import printsuccess, id_generator, printerror, getTime, printwarning, random_data, get_insert_message
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
        def __get_next_block_index(current_message_count):
            try:
                time_left = self.end_time - time.time()
                if time_left <= MESSENGER_SCHEDULER_FREQ:
                    return EXPERIMENT_MESSAGE_COUNT
                else:
                    messages_left = EXPERIMENT_MESSAGE_COUNT - current_message_count
                    message_blocks = time_left / MESSENGER_SCHEDULER_FREQ
                    return current_message_count + int(messages_left / message_blocks)
            except Exception as e:
                printerror(f'Unable to get next block size: {e}')
                return EXPERIMENT_MESSAGE_COUNT

        def __build_message_list():
            def __build_message(fraction):
                def __get_scheduled_timedelta(fraction):
                    try:
                        upper = self.end_time - time.time()
                        lower = int(upper * fraction)
                        return datetime.timedelta(seconds=randrange(lower, int(upper) + 1))
                    except Exception as e:
                        printerror(f'Unable to calculate time: {e}')
                        return datetime.timedelta(seconds=int(self.end_time - time.time()))

                time_to_send = datetime.datetime.now() + __get_scheduled_timedelta(fraction)
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

            message_block_count = int(EXPERIMENT_DURATION_HOURS * 3600 / MESSENGER_SCHEDULER_FREQ)
            message_block_size = int(EXPERIMENT_MESSAGE_COUNT / message_block_count)

            msg_count = 0
            messages = []
            while msg_count <= EXPERIMENT_MESSAGE_COUNT:
                for i in range(message_block_size):
                    messages.append(__build_message(msg_count / EXPERIMENT_MESSAGE_COUNT))
                msg_count += message_block_size
            
            return messages[:EXPERIMENT_MESSAGE_COUNT]
        
        def __kafka_send(message_list):
            def __kafka_header(headers):
                return [(k, bytes(v, 'utf-8')) for k, v in headers.items()]

            producer = Producer({ 'bootstrap.servers': KAFKA_SERVER, 'client.id' : self.job_id })
            message_sent = 0
            kafka_progress = ProgressInfo('Kafka', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                ul = __get_next_block_index(message_sent)
                start_time = time.time()
                for message in message_list[message_sent:ul]:
                    while True:
                        try:
                            message['header'][EXPERIMENT_MESSAGE_CREATION_KEY] = getTime()
                            producer.produce(topic=SM_TOPIC, value=bytes(message['value'], 'utf-8'), headers=__kafka_header(message['header']))
                            break
                        except Exception as e:
                            time.sleep(0.01)

                producer.flush()
                message_sent = ul
                kafka_progress.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            printsuccess(f'Kafka messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')

        def __database_scheduler_send(message_list):
            while True:
                try:
                    cluster = Cluster([DATABASE_SCHEDULER_HOST])
                    session = cluster.connect()
                    session.set_keyspace(DATABASE_SCHEDULER_KEYSPACE.lower())
                    break
                except Exception as e:
                    time.sleep(0.01)

            message_sent = 0
            database_progress = ProgressInfo('DB Scheduler', EXPERIMENT_MESSAGE_COUNT)

            while message_sent < EXPERIMENT_MESSAGE_COUNT:
                ul = __get_next_block_index(message_sent)
                start_time = time.time()
                for message in message_list[message_sent:ul]:
                    while True:
                        try:
                            message['header'][EXPERIMENT_MESSAGE_CREATION_KEY] = getTime()
                            insert_keys, insert_string = get_insert_message(message)
                            session.execute(f"INSERT INTO {DATABASE_SCHEDULER_SM_TABLE.lower()} ({insert_keys}) VALUES ({insert_string})")
                            break
                        except Exception as e:
                            time.sleep(0.01)
                message_sent = ul
                database_progress.update(message_sent)
                time.sleep(max(MESSENGER_SCHEDULER_FREQ - time.time() + start_time, 0))
            
            cluster.shutdown()
            printsuccess(f'DB Scheduler messages sent: {message_sent}, time: {(time.time() - self.start_time)/60:.2f} minutes')

        def __message_database_send(message_list):
            # This function was written to make sure no messages are being lost by Kafka or Cassandra.
            # Multiple experiment run showed there was no data loss.
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
        
        message_list = __build_message_list()

        printsuccess(f'Preparing to send {len(message_list)} messages in {EXPERIMENT_DURATION_HOURS} hours ({int(EXPERIMENT_DURATION_HOURS*60)} minutes)')

        if KAFKA_ENABLED:
            kafka_process = multiprocessing.Process(target=__kafka_send, args=(message_list,))
            kafka_process.start()
        if DATABASE_SCHEDULER_ENABLED:
            database_scheduler_process = multiprocessing.Process(target=__database_scheduler_send, args=(message_list,))
            database_scheduler_process.start()
        if MESSAGE_DATABASE_ENABLED:
            message_database_process = multiprocessing.Process(target=__message_database_send, args=(message_list,))
            message_database_process.start()
        
        time.sleep(EXPERIMENT_DURATION_HOURS * 3600)

        while (KAFKA_ENABLED and kafka_process.is_alive()) or (DATABASE_SCHEDULER_ENABLED and database_scheduler_process.is_alive()) or (MESSAGE_DATABASE_ENABLED and message_database_process.is_alive()):
            printwarning(f'Waiting for processes to finish - Kafka: {kafka_process.is_alive()} - Database Scheduler: {database_scheduler_process.is_alive()} - Message Database: {message_database_process.is_alive()}')
            time.sleep(MESSENGER_SCHEDULER_FREQ)
        
        self.finish_time = time.time()
        printsuccess(f'Messenger finished sending {len(message_list)} messages in {int((self.finish_time - self.start_time)/(60))} minutes.')


if __name__ == '__main__':
    messenger = Messenger()
    messenger.start()
    messenger.join()