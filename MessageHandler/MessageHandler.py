from common import Config, getTime, printerror, printsuccess, printdebug
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from datetime import datetime
import threading

class MessageHandler:
    threads = 0

    def __init__(self):
        self.thread_id = MessageHandler.threads
        MessageHandler.threads = MessageHandler.threads + 1
    
    def run(self):
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
                
                # If the message is scheduled for the future, then we need to bucket it
                for bucket in reversed(Config.get('bucket_object_list')):
                    if time_diff > bucket['lower']:
                        return bucket['name']

                return headers['topic']
            except Exception as e:
                printerror(f'Unable to get topic name from headers: {headers}')
                printerror(e)
                return False

        while True:
            try:
                consumer = KafkaConsumer(
                    Config.get('sm_topic'),
                    bootstrap_servers=[Config.get('kafka_server')],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=Config.get('sm_consumer_group_name'),
                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                producer = KafkaProducer(bootstrap_servers=[Config.get('kafka_server')],
                    value_serializer=lambda x: dumps(x).encode('utf-8'))
                
                for message in consumer:
                    topic = __get_topic(getattr(message, 'headers', None))
                    
                    # printdebug(f'MessageHandler.run(): Topic: {topic}')
                    # printdebug(f'MessageHandler.run(): Message: {message.value}')
                    message.value['__sm_mh_time'] = getTime()

                    if topic:
                        producer.send(
                            topic   = topic,
                            value   = getattr(message, 'value', None),
                            key     = getattr(message, 'key', None),
                            headers = getattr(message, 'headers', None)
                        )
                    else:
                        printerror(f'Unable to get topic name from headers: {message}')
            except Exception as e:
                printerror(e)
                printerror(f'Worker {self.thread_id} terminated. Restarting...')

if __name__ == '__main__':

    mh = MessageHandler()
    mh.run()