import multiprocessing
from constants import *
from common import getTime, id_generator, printinfo, printsuccess, random_data, printerror, get_insert_message, printwarning
import time
import datetime
from random import randrange
from confluent_kafka import Producer
from pymongo import MongoClient
from cassandra.cluster import Cluster

class ProgressInfo:
    def __init__(self, total) -> None:
        self.total = total
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
            printsuccess(f'Progress: {message}')

class Messenger(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)

        self.job_id = id_generator()

        self.messages_sent = 0
        self.start_time = time.time()
        self.end_time = time.time() + (EXPERIMENT_DURATION_HOURS * 3600)
        self.progress = ProgressInfo(EXPERIMENT_MESSAGE_COUNT)
    
    def run(self):

        def __get_message_to_send_count():
            try:
                total_messages_to_send = EXPERIMENT_MESSAGE_COUNT - self.messages_sent

                if total_messages_to_send < 0:
                    return 0
                
                total_time_to_send = self.end_time - time.time()
                if total_time_to_send <= MESSENGER_SCHEDULER_FREQ:
                    return EXPERIMENT_MESSAGE_COUNT - self.messages_sent

                batches_needed = total_time_to_send / MESSENGER_SCHEDULER_FREQ

                return int(total_messages_to_send / batches_needed)

            except Exception as e:
                printerror(f'Unable to get message count to send: {e}')
                return 1

        def __send_message(count):
            def __kafka_header(headers):
                return [(k, bytes(v, 'utf-8')) for k, v in headers.items()]

            def __build_message():
                def __random_time():
                    try:
                        return randrange(1, int(self.end_time - time.time()))
                    except Exception as e:
                        return 1

                time_to_send = datetime.datetime.now() + datetime.timedelta(seconds=__random_time())
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

            message_list = [ __build_message() for _ in range(count) ]

            # KAFKA
            if KAFKA_ENABLED:
                def __kafka_send(message_list):
                    producer = Producer({ 'bootstrap.servers':KAFKA_SERVER, 'client.id' : self.job_id })
                    msg_errors = 0
                    for message in message_list:
                        try:
                            producer.produce(topic=SM_TOPIC, value=bytes(message['value'], 'utf-8'), headers=__kafka_header(message['header']))
                        except Exception as e:
                            msg_errors += 1
                    if msg_errors > 0:
                        printwarning(f'{msg_errors} messages failed to send to Kafka')
                    producer.flush(KAFKA_MESSAGE_TIMEOUT)
                kafka_process = multiprocessing.Process(target=__kafka_send, args=(message_list,))
                kafka_process.start()
                    
            # Database Scheduler
            if DATABASE_SCHEDULER_ENABLED:
                def __database_scheduler_send(message_list):
                    cluster = Cluster([DATABASE_SCHEDULER_HOST])
                    session = cluster.connect()
                    session.set_keyspace(DATABASE_SCHEDULER_KEYSPACE.lower())
                    msg_errors = 0
                    for message in message_list:
                        try:
                            insert_keys, insert_string = get_insert_message(message)
                            session.execute(f"INSERT INTO {DATABASE_SCHEDULER_SM_TABLE.lower()} ({insert_keys}) VALUES ({insert_string})")
                        except Exception as e:
                            msg_errors += 1
                    if msg_errors > 0:
                        printwarning(f'{msg_errors} messages failed to send to mysql DB')
                    cluster.shutdown()
                scheduler_process = multiprocessing.Process(target=__database_scheduler_send, args=(message_list,))
                scheduler_process.start()

            # Message Database
            if MESSAGE_DATABASE_ENABLED:
                def __message_database_send(message_list):
                    message_database = MongoClient(MESSAGE_DATABASE_URL)[KAFKA_MESSAGE_TOPIC][KAFKA_MESSAGE_TOPIC]
                    msg_errors = 0
                    for message in message_list:
                        try:
                            message_database.insert_one(message['header'])
                        except Exception as e:
                            msg_errors += 1
                    if msg_errors > 0:
                        printwarning(f'{msg_errors} messages failed to send to MongoDB')
                message_process = multiprocessing.Process(target=__message_database_send, args=(message_list,))
                message_process.start()
            
            time.sleep(MESSENGER_SCHEDULER_FREQ)

            KAFKA_ENABLED and kafka_process.terminate()
            DATABASE_SCHEDULER_ENABLED and scheduler_process.terminate()
            MESSAGE_DATABASE_ENABLED and message_process.terminate()

        printsuccess(f'Messenger started')
        printinfo(f'Preparing to send {EXPERIMENT_MESSAGE_COUNT} messages in {EXPERIMENT_DURATION_HOURS} hours ({int(EXPERIMENT_DURATION_HOURS*60)} minutes)')
        while self.messages_sent < EXPERIMENT_MESSAGE_COUNT:

            # calculate how many messages to send in the next {MESSENGER_SCHEDULER_FREQ} seconds
            msg_to_send_count = __get_message_to_send_count()

            # send the messages
            __send_message(msg_to_send_count)
            
            # update the message count
            self.messages_sent += msg_to_send_count

            # update the progress message
            self.progress.update(self.messages_sent)

        self.finish_time = time.time()
        printsuccess(f'Messenger finished sending {self.messages_sent} messages in {int((self.finish_time - self.start_time)/(60))} minutes.')

if __name__ == '__main__':
    m = Messenger()
    m.start()
    m.join()
