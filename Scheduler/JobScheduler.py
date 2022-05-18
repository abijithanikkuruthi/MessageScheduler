from common import id_generator, getTime, printinfo, printerror, printsuccess, get_config, TimeoutLock
from config import JOB_TIME_FORMAT, JOB_STATUS_LIST, SM_MINIUMUM_DELAY, JOB_LOCK_TIMEOUT
import threading
import time
import copy

class Job:
    def __init__(self, bucket) -> None:
        self.job_id = id_generator()
        self.name = bucket['name']
        self.lower = bucket['lower']
        self.upper = bucket['upper']
        self.creation_time = getTime(JOB_TIME_FORMAT)
        self.status = JOB_STATUS_LIST[0]
        self.workers = []

        printinfo(f'Job Created: {self.__dict__}')
    
    def start(self, worker_id) -> None:
        self.start_time = getTime(JOB_TIME_FORMAT)
        self.workers.append(worker_id)
        self.status = JOB_STATUS_LIST[1]

        printinfo(f'Job Started by worker {worker_id}: {self.__dict__}')

    def done(self, worker_id) -> None:
        self.workers.remove(worker_id)
        self.status = JOB_STATUS_LIST[1]

        printinfo(f'Job done by worker {worker_id}: {self.__dict__}')

        if len(self.workers) == 0:
            # Mark job as done if all workers are done

            self.finish_time = getTime(JOB_TIME_FORMAT)
            self.status = JOB_STATUS_LIST[2]

            printinfo(f'Job Done: {self.__dict__}')

    def error(self, worker_id) -> None:
        self.workers.remove(worker_id)
        self.status = JOB_STATUS_LIST[-1]

        printerror(f'Worker: {worker_id}; Job: {self.__dict__}')

    def isDone(self):
        return self.status == JOB_STATUS_LIST[2]

class JobScheduler(threading.Thread):
    JOB_QUEUE = []
    JQ_LOCK = TimeoutLock()
    
    def __init__(self):
        threading.Thread.__init__(self)
        config_obj = get_config()

        self.job_stage = 0
        self.config = {
            'bucket_object_list' : config_obj['bucket_object_list'],
            'min_size' : SM_MINIUMUM_DELAY,
            'max_size' : config_obj['bucket_object_list'][-1]['lower']
        }

        printinfo(f'Job Scheduler Config: {self.config}')

    def run(self):
        printsuccess(f'Job Scheduler Started in thread ID: {threading.get_native_id()}')

        def __assign_jobs():
            def __filter_jobs(j):
                return j['lower'] <= self.job_stage and (self.job_stage % j['lower'] == 0)

            if self.job_stage == 0:
                job_list = self.config['bucket_object_list']
            else:
                job_list = filter(__filter_jobs, self.config['bucket_object_list'])
            
            self.job_stage = (self.job_stage + self.config['min_size']) % self.config['max_size']

            job_object_list = [Job(i) for i in job_list]

            with JobScheduler.JQ_LOCK.acquire_timeout(JOB_LOCK_TIMEOUT, 'JobScheduler') as acquired:
                JobScheduler.JOB_QUEUE = JobScheduler.JOB_QUEUE + job_object_list

        while True:
            threading.Thread(target=__assign_jobs).start()
            time.sleep(self.config['min_size'])
        
    @classmethod
    def start_job(cls, job_id, worker_id):
        def __start_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.start(worker_id)
                    break

        with cls.JQ_LOCK.acquire_timeout(JOB_LOCK_TIMEOUT, '__start_job()') as acquired:
            __start_job()

    @classmethod
    def done_job(cls, job_id, worker_id):
        def __done_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.done(worker_id)
                    if j.isDone():
                        cls.JOB_QUEUE = list(filter(lambda j: j.job_id != job_id, cls.JOB_QUEUE))
                    break

        with cls.JQ_LOCK.acquire_timeout(JOB_LOCK_TIMEOUT, '__done_job') as acquired:
            __done_job()
    
    @classmethod
    def error_job(cls, job_id, worker_id):
        def __error_job():
            for j in cls.JOB_QUEUE:
                if j.job_id == job_id:
                    j.error(worker_id)
                    break

        with cls.JQ_LOCK.acquire_timeout(JOB_LOCK_TIMEOUT, '__error_job()') as acquired:
            __error_job()

    @ classmethod
    def get_job_queue(cls) -> list:
        with cls.JQ_LOCK.acquire_timeout(JOB_LOCK_TIMEOUT, 'get_job_queue()') as acquired:
            jq = copy.deepcopy(cls.JOB_QUEUE)

        return jq

if __name__=='__main__':

    js = JobScheduler()
    js.start()

    time.sleep(2)

    print(list(JobScheduler.JOB_QUEUE))
    JobScheduler.start_job(JobScheduler.JOB_QUEUE[0].job_id, 'worker_id')
    print(list(JobScheduler.JOB_QUEUE))
    JobScheduler.done_job(JobScheduler.JOB_QUEUE[0].job_id, 'worker_id')