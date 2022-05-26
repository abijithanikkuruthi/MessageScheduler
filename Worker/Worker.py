import threading
import multiprocessing
from datetime import datetime
from common import Config, printheader, printsuccess, printdebug, printerror, getTime, excpetion_info
from confluent_kafka import Consumer, Producer, TopicPartition

class Task(multiprocessing.Process):
    task_id = 0

    def __init__(self, task, worker_success_value):
        multiprocessing.Process.__init__(self)
        self.task = task
        self.worker_success_value = worker_success_value
        self.task_id = Task.task_id
        Task.task_id += 1
    
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
                
                if time_diff < -1 * Config.get('sm_miniumum_delay'):
                    printerror(f'Task.run(): WORKER DELAY of {time_diff}: {headers}')

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

        def __run():
            
            group_id = Config.get('sm_consumer_group_prefix') + self.task['job']['name'] + '_' + str(self.task_id)
            
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

                message = consumer.poll(timeout=Config.get('worker_consumer_timeout'))
                
                if message is None or message.error():
                    message and message.error() and printerror(f'Task.run() MessageError: {message.error()}')
                    break
                
                headers = message.headers()
                topic = __get_topic(headers)
                hop_count = __get_header(headers, Config.get('sm_header_message_hopcount_key'))
                hop_count = (int(hop_count) + 1) if hop_count else 0

                if topic:
                    message_job_id = __get_header(headers, Config.get('sm_header_job_id_key'))

                    message.set_headers(__set_header(message.headers(), [(Config.get('sm_header_job_id_key'), bytes(self.task['job_id'], 'utf-8')),
                                         (Config.get('sm_header_worker_timestamp_key'), bytes(getTime(), 'utf-8')),
                                         (Config.get('sm_header_message_hopcount_key'), bytes(str(hop_count), 'utf-8'))]))

                    producer.produce(
                        topic   = topic,
                        value   = message.value(),
                        key     = message.key(),
                        headers = message.headers(),
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
            self.worker_success_value.value = False

class Worker(multiprocessing.Process):
    def __init__(self, work, WorkerHandler):
        multiprocessing.Process.__init__(self)
        self.task = work
        self.worker_handler = WorkerHandler
        self.worker_success_value = multiprocessing.Value('i', 1)

    def run(self):
        TaskProcessList = []
        # One process per partition
        for _ in range(Config.get('sm_partitions_per_bucket')):
            t = Task(self.task, self.worker_success_value)
            t.start()
            TaskProcessList.append(t)
        
        for t in TaskProcessList:
            t.join()
        
        self.task['status'] = Config.get('worker_status_list.done')
        
        if not self.worker_success_value.value:
            self.task['status'] = Config.get('worker_status_list.error')
            printerror(f'Worker.run(): Worker failed {self.task}')
        
        self.worker_handler.post_worker(self.task)

if __name__=='__main__':
    printheader('Worker Service')
    