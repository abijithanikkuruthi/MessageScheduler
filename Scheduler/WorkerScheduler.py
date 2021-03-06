from JobScheduler import JobScheduler
from common import getTime, TimeoutLock, printsuccess, printerror, printdebug, printwarning
from constants import WORKER_STALE_TIME, WORKER_TIME_FORMAT, WORKER_SCHEDULER_FREQ, WORKER_QUEUE_THREAD_LOCK_TIMEOUT, WORKER_STATUS_LIST
import threading
import time
import copy
from datetime import datetime

class Worker:

    def __init__(self, worker) -> None: 
        self.worker_id = worker['worker_id']
        self.join_time = getTime(WORKER_TIME_FORMAT)
        self.ip = worker.get('ip', '')
        self.update(worker)

    def update(self, worker) -> None:
        self.last_heartbeat = getTime(WORKER_TIME_FORMAT)
        self.status = worker.get('status', WORKER_STATUS_LIST.READY)
        self.job_id = worker.get('job_id', '')
        self.job = worker.get('job', {})

    def poll(self):
        self.last_heartbeat = getTime(WORKER_TIME_FORMAT)

class WorkerScheduler(threading.Thread):

    WORKER_QUEUE = {}
    WQ_LOCK = TimeoutLock('WORKER_QUEUE_LOCK')

    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        printsuccess(f'WorkerScheduler Started in thread ID: {threading.get_native_id()}')
        time.sleep(WORKER_SCHEDULER_FREQ)

        def __run():
            printdebug(f'WorkerScheduler CronJob Started in thread ID: {threading.get_native_id()}')

            # Remove stale workers, mark associated jobs as error, call jobqueue trim()
            with WorkerScheduler.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.__run()') as acquired:
                if len(WorkerScheduler.WORKER_QUEUE) == 0:
                    printwarning(f'WorkerScheduler.__run(): No Workers Available')
                    
                for worker_id in list(WorkerScheduler.WORKER_QUEUE):
                    worker = WorkerScheduler.WORKER_QUEUE[worker_id]
                    time_diff = (datetime.now() - datetime.strptime(worker.last_heartbeat, WORKER_TIME_FORMAT)).total_seconds()
                    
                    if time_diff > WORKER_STALE_TIME:
                        printerror(f'Worker: {worker_id} is stale, removing from Worker Queue')
                        if worker.job_id:
                            JobScheduler.error_job(worker.job_id, worker_id)
                        del WorkerScheduler.WORKER_QUEUE[worker_id]

                wq_copy = { k:v.__dict__ for k, v in copy.deepcopy(WorkerScheduler.WORKER_QUEUE).items() }
                JobScheduler.trim_job_queue(wq_copy)

        while True:
            try:
                threading.Thread(target=__run).start()
            except Exception as e:
                printerror(f'WorkerScheduler.__run() failed in thread ID: {threading.get_native_id()} - {str(e)}')
            finally:
                time.sleep(WORKER_SCHEDULER_FREQ)

    @classmethod
    def update(cls, worker):
        with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
            if worker['worker_id'] not in cls.WORKER_QUEUE.keys():
                cls.WORKER_QUEUE[worker['worker_id']] = Worker(worker)
        
        job = None
        # READY - Add top job to worker
        if worker['status'] == WORKER_STATUS_LIST.READY:
            job = JobScheduler.add_worker(worker['worker_id'])
            if job:
                worker['job'] = job
                worker['job_id'] = job['job_id']
                worker['status'] = WORKER_STATUS_LIST.WORKING

                with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                    cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST.WORKING:
            job = JobScheduler.start_job(worker['job_id'], worker['worker_id'])
    
            worker['job'] = job
            worker['job_id'] = job and job['job_id']
            
            with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST.DONE:
            JobScheduler.done_job(worker['job_id'], worker['worker_id'])
            worker['job'] = None
            worker['job_id'] = None
            worker['status'] = WORKER_STATUS_LIST.READY
            with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST.ERROR:
            JobScheduler.error_job(worker['job_id'], worker['worker_id'])
            worker['job'] = None
            worker['job_id'] = None
            worker['status'] = WORKER_STATUS_LIST.READY
            with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)

        with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
            w_obj = copy.deepcopy(cls.WORKER_QUEUE[worker['worker_id']].__dict__)
        
        return w_obj
    
    @classmethod
    def get_worker_queue(cls):
        with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.get_worker_queue()') as acquired:
            w_queue = copy.deepcopy(cls.WORKER_QUEUE)
        for worker_id in w_queue:
            w_queue[worker_id] = w_queue[worker_id].__dict__
        return w_queue
    
    @classmethod
    def get_job_id(cls, worker_id):
        job_id = None
        with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.get_job_id()') as acquired:
            if worker_id in cls.WORKER_QUEUE:
                job_id = copy.deepcopy(cls.WORKER_QUEUE[worker_id].job_id)

        return job_id
    
    @classmethod
    def get_worker(cls, worker):
        with cls.WQ_LOCK.acquire_timeout(WORKER_QUEUE_THREAD_LOCK_TIMEOUT, 'WorkerScheduler.get_worker()') as acquired:
            if worker['worker_id'] not in cls.WORKER_QUEUE.keys():
                cls.WORKER_QUEUE[worker['worker_id']] = Worker(worker)
            else:
                cls.WORKER_QUEUE[worker['worker_id']].poll()
            worker = copy.deepcopy(cls.WORKER_QUEUE[worker['worker_id']].__dict__)

        return worker

if __name__=="__main__":

    ws = WorkerScheduler()
    js = JobScheduler()
    ws.start()
    
    wkr = {
        'worker_id': 'wkr1',
        'status': 'READY',
    }

    wkr = WorkerScheduler.update(wkr)

    print(wkr)

    js.start()
    
    time.sleep(2)
    wkr = WorkerScheduler.update(wkr)

    print(wkr)

    wkr['status'] = 'WORKING'
    wkr = WorkerScheduler.update(wkr)
    print(wkr)

    wkr['status'] = 'DONE'
    wkr = WorkerScheduler.update(wkr)
    print(wkr)