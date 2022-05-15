from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from common import printinfo, printerror, get_bucket_object_list
from datetime import datetime

class MessageHandler:
    workers = 0

    def __init__(self):
        self.worker_id = MessageHandler.workers
        self.bucket_list = get_bucket_object_list()
        MessageHandler.workers = MessageHandler.workers + 1
    
    def run(self, config):
        def __get_topic(headers):
            if (not headers) or (not headers.topic):
                return False
            if (not headers.time):
                return headers.topic

            time_diff = (datetime.strftime(headers.time, config.time_format) - datetime.now()).total_seconds()
            
            # get topic name from self.bucket_list { 'name'}

            return 'ss' 

        while True:
            try:
                printinfo(f'Starting worker: {self.worker_id}')

                consumer = KafkaConsumer(
                    config.sm_topic,
                    bootstrap_servers=[config.kafka_server],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=config.group,
                    value_deserializer=lambda x: loads(x.decode('utf-8')))
                
                producer = KafkaProducer(bootstrap_servers=[config.kafka_server],
                    value_serializer=lambda x: dumps(x).encode('utf-8'))
                
                for message in consumer:
                    topic = __get_topic(getattr(message, 'headers', None))
                    if topic:
                        producer.send(
                            topic   = topic,
                            value   = message.value,
                            key     = getattr(message, 'key', None),
                            headers = getattr(message, 'headers', None)
                        )
            except:
                printerror(f'Worker {self.worker_id} terminated. Restarting...')

if __name__ == '__main__':

    mh = MessageHandler()
    mh.run()