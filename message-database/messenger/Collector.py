import multiprocessing
from constants import *
from confluent_kafka import Consumer
from common import getTime, id_generator, printinfo, printsuccess, printerror, random_data
import threading
from pymongo import MongoClient

class CollectorProcess(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):

        while True:
            try:
                message_database = MongoClient(MESSAGE_DATABASE_URL)[KAFKA_MESSAGE_TOPIC][MESSAGE_DATABASE_TABLE] if MESSAGE_DATABASE_ENABLED else None
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
                    else:
                        message_count += 1
                        message_database.insert_one({ i[0]:i[1].decode() for i in message.headers()})

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
        if MESSAGE_DATABASE_ENABLED:
            for _ in range(COLLECTOR_PROCESS_COUNT):
                CollectorProcess().start()
            printsuccess(f'Collector started in {COLLECTOR_PROCESS_COUNT} processes')