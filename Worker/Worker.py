import threading
import multiprocessing
from datetime import datetime
from common import Config, printheader, printsuccess, printdebug, printerror, WorkerHandler
import time

worker_success_value = multiprocessing.Value('i', 0)

class Task(multiprocessing.Process):
    def __init__(self, task):
        multiprocessing.Process.__init__(self)
        self.task = task
    
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

        def __run():
            time.sleep(1)
            pass
        
        printsuccess(f'Task Started in thread ID: {threading.get_native_id()}')

        try:
            __run()
        except Exception as e:
            printerror(f'Task Failed in thread ID: {threading.get_native_id()}')
            printerror(e)
            worker_success_value.value = False

class Worker(multiprocessing.Process):
    def __init__(self, work):
        multiprocessing.Process.__init__(self)
        self.task = work
        worker_success_value.value = True

    def run(self):
        printsuccess(f'Worker started in PID: {threading.get_native_id()}')
        
        TaskThreadList = []
        for _ in range(Config.get('worker_thread_count')):
            t = Task(self.task)
            t.start()
            TaskThreadList.append(t)
        
        for t in TaskThreadList:
            t.join()
        
        self.task['status'] = Config.get('worker_status_list')[2]
        
        if not worker_success_value.value:
            self.task['status'] = Config.get('worker_status_list')[-1]
            printerror('Worker.run(): Worker failed {self.task}')
        
        WorkerHandler.post_worker(self.task)

if __name__=='__main__':
    printheader('Worker Service')
    
    worker_thread = Worker(WorkerHandler.post_worker(WorkerHandler.get_worker()))
    
    worker_thread.start()