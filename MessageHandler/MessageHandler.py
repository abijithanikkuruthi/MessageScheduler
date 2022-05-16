from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from common import printinfo, printerror, get_bucket_object_list, printsuccess
from datetime import datetime

class MessageHandler:
    workers = 0

    def __init__(self):
        self.worker_id = MessageHandler.workers
        self.bucket_list = get_bucket_object_list()
        MessageHandler.workers = MessageHandler.workers + 1
    
    def run(self, config):
        def __get_topic(headers_tuple):
            try:
                headers = dict((k, v.decode('utf-8')) for k, v in headers_tuple)
                if (not headers) or (not headers['topic']):
                    return False
                if (not headers['time']):
                    return headers['topic']

                time_diff = (datetime.strptime(headers['time'], config['time_format']) - datetime.now()).total_seconds()
                
                # If the message is scheduled for a time too small to bucket, we send it to the provided topic
                if time_diff < self.bucket_list[0]['lower']:
                    return headers['topic']
                
                # If the message is scheduled for the future, then we need to bucket it
                for bucket in reversed(self.bucket_list):
                    if time_diff > bucket['lower']:
                        return bucket['name']

                return headers['topic']
            except Exception as e:
                printerror(f'Unable to get topic name from headers: {headers}')
                printerror(e)
                return False

        while True:
            try:
                printsuccess(f'Starting worker: {self.worker_id}')

                consumer = KafkaConsumer(
                    config['sm_topic'],
                    bootstrap_servers=[config['kafka_server']],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=config['group'],
                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                producer = KafkaProducer(bootstrap_servers=[config['kafka_server']],
                    value_serializer=lambda x: dumps(x).encode('utf-8'))
                
                for message in consumer:
                    topic = __get_topic(getattr(message, 'headers', None))
                    if topic:
                        producer.send(
                            topic   = topic,
                            value   = getattr(message, 'value', None),
                            key     = getattr(message, 'key', None),
                            headers = getattr(message, 'headers', None)
                        )
            except Exception as e:
                printerror(f'Error in processing: {message}')
                printerror(e)
                printerror(f'Worker {self.worker_id} terminated. Restarting...')

if __name__ == '__main__':
    from common import get_config

    mh = MessageHandler()
    mh.run(get_config())