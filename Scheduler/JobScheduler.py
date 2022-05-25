from common import id_generator, getTime, printerror, printinfo, printsuccess, printdebug, TimeoutLock, get_bucket_object_list
from constants import *
import threading
import time
import copy

class Job:
    JOB_LOG_LOCK = TimeoutLock('JOB_LOG_LOCK')

    def __init__(self, bucket) -> None:
        self.job_id = id_generator()
        self.name = bucket['name']
        self.lower = bucket['lower']
        self.upper = bucket['upper']
        self.creation_time = getTime(JOB_TIME_FORMAT)
        self.status = JOB_STATUS_LIST.READY
        self.workers = []

        printdebug(f'Job Created: {self.__dict__}')
    
    def start(self, worker_id) -> None:
        if worker_id not in self.workers:
            self.workers.append(worker_id)
        self.status = JOB_STATUS_LIST.WORKING

        printdebug(f'Job Started by worker {worker_id}: {self.__dict__}')

    def done(self, worker_id) -> None:
        self.workers.remove(worker_id)
        self.status = JOB_STATUS_LIST.WORKING

        printdebug(f'Job done by worker {worker_id}: {self.__dict__}')

        if len(self.workers) == 0:
            # Mark job as done if all workers are done

            self.finish_time = getTime(JOB_TIME_FORMAT)
            self.status = JOB_STATUS_LIST.DONE

            with Job.JOB_LOG_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'Job.done()') as acquired:
                job_log_str = ', '.join([str(v) for k, v in self.__dict__.items()])
                try:
                    with open(JOB_LOG_FILE, 'a') as f:
                        f.write(f'{job_log_str}\n')
                except Exception as e:
                    printinfo(f'Job.done(): Error writing to Job Log File: {e}')
                    with open(JOB_LOG_FILE, 'a+') as f:
                        job_header_str = ', '.join(self.__dict__.keys())
                        f.write(f'{job_header_str}\n')
                        f.write(f'{job_log_str}\n')

            printdebug(f'Job Done: {self.__dict__}')

    def error(self, worker_id) -> None:
        self.workers.remove(worker_id)
        self.status = JOB_STATUS_LIST.ERROR

        printerror(f'Worker: {worker_id}; Job: {self.__dict__}')

    def isDone(self):
        return self.status == JOB_STATUS_LIST.DONE

    @classmethod
    def get_job_log(cls):
        with Job.JOB_LOG_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'Job.get_job_log()') as acquired:
            try:
                with open(JOB_LOG_FILE, 'r') as f:
                    return f.read()
            except Exception as e:
                printinfo(f'Job.get_job_log(): Error reading Job Log File: {e}')
                return ''

class JobScheduler(threading.Thread):
    JOB_QUEUE = []
    JQ_LOCK = TimeoutLock('JOB_QUEUE_LOCK')
    
    def __init__(self):
        threading.Thread.__init__(self)

        self.job_stage = 0
        self.config = {
            'bucket_object_list' : get_bucket_object_list(),
            'min_size' : SM_MINIUMUM_DELAY,
            'max_size' : get_bucket_object_list()[-1]['lower']
        }

        printdebug(f'Job Scheduler Config: {self.config}')

    def run(self):
        printsuccess(f'Job Scheduler Started in thread ID: {threading.get_native_id()}')

        def __run():
            printdebug(f'Job Scheduler CronJob Running in thread ID: {threading.get_native_id()}')

            def __assign_jobs():
                def __filter_jobs(j):
                    return j['lower'] <= self.job_stage and (self.job_stage % j['lower'] == 0)

                if self.job_stage == 0:
                    job_list = self.config['bucket_object_list']
                else:
                    job_list = filter(__filter_jobs, self.config['bucket_object_list'])
                
                self.job_stage = (self.job_stage + self.config['min_size']) % self.config['max_size']

                job_object_list = [Job(i) for i in job_list]

                with JobScheduler.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'JobScheduler') as acquired:
                    JobScheduler.JOB_QUEUE = JobScheduler.JOB_QUEUE + job_object_list
            
            __assign_jobs()

        while True:
            threading.Thread(target=__run).start()
            time.sleep(self.config['min_size'])
        
    @classmethod
    def start_job(cls, job_id, worker_id):
        job_obj = None
        def __start_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.start(worker_id)
                    return copy.deepcopy(j.__dict__)
            return None

        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, '__start_job()') as acquired:
            job_obj = __start_job()
        
        return job_obj

    @classmethod
    def done_job(cls, job_id, worker_id):
        def __done_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.done(worker_id)
                    if j.isDone():
                        cls.JOB_QUEUE = list(filter(lambda j: j.job_id != job_id, cls.JOB_QUEUE))
                    break

        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, '__done_job') as acquired:
            __done_job()
    
    @classmethod
    def error_job(cls, job_id, worker_id):
        def __error_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.error(worker_id)
                    break

        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, '__error_job()') as acquired:
            __error_job()

    @ classmethod
    def get_job_queue(cls) -> list:
        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'get_job_queue()') as acquired:
            jq = copy.deepcopy(cls.JOB_QUEUE)

        return [j.__dict__ for j in jq]

    @classmethod
    def trim_job_queue(cls, worker_queue):
        # TODO: Implement trimming of workers in job object with worker_queue
        
        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'trim_job_queue()') as acquired:
            cls.JOB_QUEUE = cls.JOB_QUEUE[-1 * JOB_QUEUE_MAX_SIZE:]

    @classmethod
    def add_worker(cls, worker_id):
        ready_list = []
        error_list = []
        working_list = []
        job_obj = None

        with cls.JQ_LOCK.acquire_timeout(JOB_QUEUE_THREAD_LOCK_TIMEOUT, 'add_worker()') as acquired:
            for j in cls.JOB_QUEUE:
                if j.status == JOB_STATUS_LIST.READY:
                    ready_list.append(j)
                elif j.status == JOB_STATUS_LIST.ERROR:
                    error_list.append(j)
                elif j.status == JOB_STATUS_LIST.WORKING and len(j.workers) == 0:
                    working_list.append(j)

            if len(ready_list) > 0:
                job_obj = ready_list[0]
                job_obj.start(worker_id)
            elif len(error_list) > 0:
                job_obj = error_list[0]
                job_obj.start(worker_id)
            elif len(working_list) > 0:
                # choose job with least number of workers
                job_obj = sorted(working_list, key= lambda j: len(j.workers))[0]
                job_obj.start(worker_id)

            return_obj = copy.deepcopy(job_obj.__dict__) if job_obj else None
    
        return return_obj

if __name__=='__main__':

    js = JobScheduler()
    js.start()

    time.sleep(2)

    print(JobScheduler.get_job_queue())
    JobScheduler.start_job(JobScheduler.get_job_queue()[0]['job_id'], 'worker_id')
    print(JobScheduler.get_job_queue())
    JobScheduler.done_job(JobScheduler.get_job_queue()[0]['job_id'], 'worker_id')