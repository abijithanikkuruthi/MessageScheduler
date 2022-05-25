import threading
import multiprocessing
from datetime import datetime
from common import Config, printheader, printsuccess, printdebug, printerror, getTime, excpetion_info
import time
# from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from json import loads, dumps
from confluent_kafka import Consumer, Producer, TopicPartition

worker_success_value = multiprocessing.Value('i', 0)

class Task(multiprocessing.Process):
    task_id = 0

    def __init__(self, task):
        multiprocessing.Process.__init__(self)
        self.task = task
        self.task_id = Task.task_id
        Task.task_id += 1
    
    def run(self):
        def __get_header(headers_tuple, key):
            for header in headers_tuple:
                if header[0] == key:
                    return header[1]
            return None

        def __set_header(headers_tuple, key, value):
            headers_tuple_updated = []
            for header in headers_tuple:
                if header[0] != key:
                    headers_tuple_updated.append(header)
            return headers_tuple_updated + [(key, value)]

        def __get_topic(headers_tuple):
            try:
                headers = dict((k, v.decode('utf-8')) for k, v in headers_tuple)
                if (not headers) or (not headers['topic']):
                    return False
                if (not headers['time']):
                    return headers['topic']

                time_diff = (datetime.strptime(headers['time'], Config.get('sm_time_format')) - datetime.now()).total_seconds()
                
                if time_diff < -2 * Config.get('sm_miniumum_delay'):
                    printerror(f'Task.run(): WORKER DELAY: {headers}')

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

        def __run():
            
            group_id = Config.get('sm_consumer_group_name') + self.task['job']['name'] + '_' + str(self.task_id)
            
            consumer = Consumer({
                'bootstrap.servers':    Config.get('kafka_server'),
                'group.id':             group_id,
                'enable.auto.commit':   True,
                'auto.offset.reset':    'earliest',
            })

            producer = Producer({
                'bootstrap.servers':    Config.get('kafka_server')})
            
            topic_partition = TopicPartition(self.task['job']['name'], self.task_id)
            consumer.assign([topic_partition])

            while True:

                message = consumer.poll(timeout=1)
                
                if message is None or message.error():
                    message and message.error() and printerror(f'Task.run() MessageError: {message.error()}')
                    break

                topic = __get_topic(message.headers())
                message_job_id = __get_header(message.headers(), Config.get('sm_header_job_id_key'))

                if topic:
                    producer.produce(
                        topic   = topic,
                        value   = message.value(),
                        key     = message.key(),
                        headers = __set_header(message.headers(), Config.get('sm_header_job_id_key'), bytes(self.task['job_id'], 'utf-8'))
                    )
                    
                    if bytes(self.task['job_id'], 'utf-8') == message_job_id:
                        break

            consumer.close()
            producer.flush()


        try:
            __run()
        except Exception as e:
            printerror(f'Task Failed in thread ID: {threading.get_native_id()}')
            printerror(e)
            worker_success_value.value = False

class Worker(multiprocessing.Process):
    def __init__(self, work, WorkerHandler):
        multiprocessing.Process.__init__(self)
        self.task = work
        self.worker_handler = WorkerHandler
        worker_success_value.value = True

    def run(self):
        TaskProcessList = []
        # One process per partition
        for _ in range(Config.get('sm_partitions_per_bucket')):
            t = Task(self.task)
            t.start()
            TaskProcessList.append(t)
        
        for t in TaskProcessList:
            t.join()
        
        self.task['status'] = Config.get('worker_status_list')[2]
        
        if not worker_success_value.value:
            self.task['status'] = Config.get('worker_status_list')[-1]
            printerror(f'Worker.run(): Worker failed {self.task}')
        
        self.worker_handler.post_worker(self.task)

if __name__=='__main__':
    printheader('Worker Service')
    