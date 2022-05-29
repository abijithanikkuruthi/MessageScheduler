import multiprocessing
from constants import *
from common import getTime, id_generator, printinfo, printsuccess, random_data, printerror
import time
import datetime
from random import randrange
from confluent_kafka import Producer

class ProgressInfo:
    def __init__(self, total) -> None:
        self.total = total
        self.current = 0
        self.progress = {
            '25': False,
            '50': False,
            '75': False,
        }
    
    def update(self, current):
        self.current = current
        percent = int(self.current / self.total * 100)
        message = None
        if percent > 25 and not self.progress['25']:
            self.progress['25'] = True
            message = '25%'
        elif percent > 50 and not self.progress['50']:
            self.progress['50'] = True
            message = '50%'
        elif percent > 75 and not self.progress['75']:
            self.progress['75'] = True
            message = '75%'

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
    
        #self.producer = Producer({ 'bootstrap.servers':KAFKA_SERVER, 'client.id' : self.job_id }) if KAFKA_ENABLED else None
        # self.producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER],
        #                 value_serializer=lambda x:  json.dumps(x).encode('utf-8'))
        # self.database_scheduler = False #DATABASE_ENABLED and DATABASE_SERVER
        # self.message_database = False #mongo.MongoClient(DATABASE_SERVER)['messages']

    def run(self):
        # Data connections init
        producer = Producer({ 'bootstrap.servers':KAFKA_SERVER, 'client.id' : self.job_id }) if KAFKA_ENABLED else None
        database_scheduler = False #DATABASE_ENABLED and DATABASE_SERVER
        message_database = False #mongo.MongoClient(DATABASE_SERVER)['messages']

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

            for _ in range(count):
                message = __build_message()

                # KAFKA
                if producer is not None:
                    message_sent = False
                    tries = 0
                    while not message_sent:
                        try:
                            producer.produce(topic=SM_TOPIC, value=bytes(message['value'], 'utf-8'), headers=__kafka_header(message['header']))
                            # self.producer.send(topic=SM_TOPIC, value=message['value'], headers=__kafka_header(message['header']))
                            message_sent = True
                            break
                        except Exception as e:
                            printerror(f'Unable to send message to kafka: {e}')
                            tries += 1
                            if tries > REQUEST_COUNT_LIMIT:
                                break
                            time.sleep(1)
                
                # Database Scheduler
                if database_scheduler:
                    message_sent = False
                    tries = 0
                    while not message_sent:
                        try:
                            # message_database.insert_one(message)
                            message_sent = True
                            break
                        except Exception as e:
                            printerror(f'Unable to send message to DB: {e}')
                            tries += 1
                            if tries > REQUEST_COUNT_LIMIT:
                                break
                            time.sleep(1)
                
                # Message Database
                if message_database:
                    message_sent = False
                    tries = 0
                    while not message_sent:
                        try:
                            # message_database.insert_one(message)
                            message_sent = True
                            break
                        except Exception as e:
                            printerror(f'Unable to send message to DB: {e}')
                            tries += 1
                            if tries > REQUEST_COUNT_LIMIT:
                                break
                            time.sleep(1)
            
            producer.flush(KAFKA_MESSAGE_TIMEOUT)
            # self.message_database.insert_one(message).commit()

        printsuccess(f'Messenger started')
        while self.messages_sent < EXPERIMENT_MESSAGE_COUNT:

            # calculate how many messages to send in the next {MESSENGER_SCHEDULER_FREQ} seconds
            msg_to_send_count = __get_message_to_send_count()

            # send the messages
            __send_message(msg_to_send_count)

            # update the message count
            self.messages_sent += msg_to_send_count

            # update the progress message
            self.progress.update(self.messages_sent)

            # sleep until the next message should be sent
            time.sleep(MESSENGER_SCHEDULER_FREQ)

        self.finish_time = time.time()
        printsuccess(f'Messenger finished sending {self.messages_sent} messages in {(self.finish_time - self.start_time)/(60*60)} hours.')

if __name__ == '__main__':
    m = Messenger()
    m.start()
    m.join()
