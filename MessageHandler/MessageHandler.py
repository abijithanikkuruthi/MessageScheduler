import threading
from common import Config, getTime, printerror, printsuccess, printdebug
from confluent_kafka import Consumer, Producer
from json import loads, dumps
from datetime import datetime

class MessageHandler:
    def __init__(self):
        pass
    
    def run(self):
        def __get_header(headers_tuple, key):
            for header in headers_tuple:
                if header[0] == key:
                    return header[1]
            return None

        def __set_header(headers_tuples, new_tuples):
            new_keys = [i[0] for i in new_tuples]
            return new_tuples + [i for i in headers_tuples if i[0] not in new_keys]

        def __get_topic(headers_tuple):
            try:
                headers = dict((k, v.decode('utf-8')) for k, v in headers_tuple)
                if (not headers) or (not headers['topic']):
                    return False
                
                if (not headers['time']):
                    return headers['topic']

                time_diff = (datetime.strptime(headers['time'], Config.get('sm_time_format')) - datetime.now()).total_seconds()
                
                # If the message is scheduled for a time too small to bucket, we send it to the provided topic
                if time_diff < Config.get('sm_miniumum_delay'):
                    return headers['topic']
                
                # If the message is scheduled for the future (+ bucket process time), then we need to bucket it in the associated bucket
                for bucket in reversed(Config.get('bucket_object_list')):
                    if time_diff > (1 + Config.get('sm_bucket_process_time_fraction')) * bucket['lower']:
                        return bucket['name']

                return headers['topic']
            except Exception as e:
                printerror(f'Unable to get topic name from headers: {headers}')
                printerror(e)
                return False

        while True:
            try:
                consumer = Consumer({
                    'bootstrap.servers':    Config.get('kafka_server'),
                    'group.id':             Config.get('sm_mh_consumer_group_id'),
                    'enable.auto.commit':   True,
                    'auto.offset.reset':    'earliest',
                })
                consumer.subscribe([Config.get('sm_topic')])
                
                producer = Producer({
                    'bootstrap.servers':    Config.get('kafka_server')})
                
                message_count = 0
                while True:
                    message = consumer.poll(timeout=Config.get('worker_consumer_timeout'))
                    
                    if message is None or message.error():
                        message and message.error() and printerror(f'{message.error()}')
                        producer.flush()
                    
                    else:
                        topic = __get_topic(message.headers())

                        if topic:
                            message_count += 1
                            message.set_headers(__set_header(message.headers(), [(Config.get('sm_header_mh_timestamp_key'), bytes(getTime(), 'utf-8')),]))

                            producer.produce(
                                topic   = topic,
                                value   = message.value(),
                                key     = message.key(),
                                headers = message.headers(),
                            )

                            (message_count % 100 == 0) and producer.flush()
                    

            except Exception as e:
                try:
                    printerror(f'MessageHandler.run(): Trying to close consumer and producer')
                    producer.flush()
                    consumer.close()
                except Exception as e:
                    printerror(f'Unable to close consumer and producer: {e}')
                printerror(e)
                printerror(f'Worker PID: {threading.get_native_id()} terminated. Restarting...')

if __name__ == '__main__':

    mh = MessageHandler()
    mh.run()