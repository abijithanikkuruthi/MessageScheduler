import multiprocessing
from constants import *
from confluent_kafka import Consumer
from common import getTime, id_generator, printinfo, printsuccess, printerror, random_data
import threading
from pymongo import MongoClient
import time

class CollectorProcess(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):

        while True:
            try:
                message_database = MongoClient(MESSENGER_DATABASE_URL)[KAFKA_MESSAGE_TOPIC][MESSENGER_DATABASE_TABLE] if MESSENGER_DATABASE_ENABLED else None
                consumer = Consumer({
                    'bootstrap.servers':    KAFKA_SERVER,
                    'group.id':             KAFKA_MESSAGE_GROUP_ID,
                    'enable.auto.commit':   True,
                    'auto.offset.reset':    'earliest',
                })
                consumer.subscribe([KAFKA_MESSAGE_TOPIC])
                
                message_count = 0
                while True:
                    message = consumer.poll(timeout=COLLECTOR_CONSUMER_TIMEOUT)
                    
                    if message is None or message.error():
                        message and message.error() and printerror(f'{message.error()}')
                        # Close the consumer for a while, then reopen it if no messages are received for a while
                        consumer.close()
                        time.sleep(COLLECTOR_CONSUMER_FREQ)
                        consumer = Consumer({
                            'bootstrap.servers':    KAFKA_SERVER,
                            'group.id':             KAFKA_MESSAGE_GROUP_ID,
                            'enable.auto.commit':   True,
                            'auto.offset.reset':    'earliest',
                        })
                        consumer.subscribe([KAFKA_MESSAGE_TOPIC])
                    else:
                        message_count += 1
                        message_database.insert_one({ i[0]:i[1].decode() for i in message.headers() if i[0] in ['time', '__sm_mh_timestamp', '__sm_worker_timestamp', '__sm_message_hopcount']})

            except Exception as e:
                try:
                    printerror(f'Collector.run(): Trying to close consumer')
                    consumer.close()
                except Exception as e:
                    printerror(f'Unable to close consumer: {e}')
                printerror(e)
                printerror(f'Worker PID: {threading.get_native_id()} terminated. Restarting...')

class Collector(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        if MESSENGER_DATABASE_ENABLED and KAFKA_ENABLED:
            for _ in range(COLLECTOR_PROCESS_COUNT):
                CollectorProcess().start()
            printsuccess(f'Collector started in {COLLECTOR_PROCESS_COUNT} processes')