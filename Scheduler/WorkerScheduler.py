from JobScheduler import JobScheduler
from MessageHandler.common import printerror
from common import getTime, TimeoutLock, printsuccess
from config import WORKER_STALE_TIME, WORKER_TIME_FORMAT, WORKER_SCHEDULER_FREQ, WORKER_LOCK_TIMEOUT, WORKER_STATUS_LIST
import threading
import time
from datetime import datetime

class Worker:

    def __init__(self, worker) -> None: 
        self.worker_id = worker['worker_id']
        self.join_time = getTime(WORKER_TIME_FORMAT)
        self.ip = getattr(worker, 'ip', '')
        self.update(worker)

    def update(self, worker) -> None:
        self.last_heartbeat = getTime(WORKER_TIME_FORMAT)
        self.status = getattr(worker, 'status', WORKER_STATUS_LIST[0])
        self.job_id = getattr(worker, 'job_id', None)
        self.job = getattr(worker, 'job', None)

class WorkerScheduler(threading.Thread):

    WORKER_QUEUE = {}
    WQ_LOCK = TimeoutLock('WORKER_QUEUE_LOCK')

    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        printsuccess(f'WorkerScheduler Started in thread ID: {threading.get_native_id()}')

        def __run():
            # Remove stale workers, mark associated jobs as error, call jobqueue trim()
            with WorkerScheduler.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.__run()') as acquired:
                for worker_id, worker in WorkerScheduler.WORKER_QUEUE.items():
                    time_diff = (datetime.now() - datetime.strptime(worker.last_heartbeat, WORKER_TIME_FORMAT)).total_seconds()
                    
                    if time_diff > WORKER_STALE_TIME:
                        if worker.job_id:
                            JobScheduler.error_job(worker.job_id, worker_id)
                        del WorkerScheduler.WORKER_QUEUE[worker_id]
            
            JobScheduler.trim_job_queue()

        while True:
            try:
                threading.Thread(target=__run()).start()
            except:
                printerror(f'WorkerScheduler.__run() failed in thread ID: {threading.get_native_id()}')
            finally:
                time.sleep(WORKER_SCHEDULER_FREQ)

    @classmethod
    def update(cls, worker):
        with cls.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
            if worker['worker_id'] not in cls.WORKER_QUEUE.keys():
                cls.WORKER_QUEUE[worker['worker_id']] = Worker(worker)
        
        job = None
        # READY - Add top job to worker
        if worker['status'] == WORKER_STATUS_LIST[0]:
            job = JobScheduler.add_worker(worker['worker_id'])
            if job:
                worker['job'] = job
                worker['job_id'] = job['job_id']
                worker['status'] = WORKER_STATUS_LIST[1]

                with cls.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                    cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST[1]:
            job = JobScheduler.start_job(worker['job_id'], worker['worker_id'])
    
            worker['job'] = job
            worker['job_id'] = job and job['job_id']
            
            with cls.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST[2]:
            JobScheduler.done_job(worker['job_id'], worker['worker_id'])
            worker['job'] = None
            worker['job_id'] = None
            worker['status'] = WORKER_STATUS_LIST[0]
            with cls.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)
        
        elif worker['status'] == WORKER_STATUS_LIST[3]:
            JobScheduler.error_job(worker['job_id'], worker['worker_id'])
            worker['job'] = None
            worker['job_id'] = None
            worker['status'] = WORKER_STATUS_LIST[0]
            with cls.WQ_LOCK.acquire_timeout(WORKER_LOCK_TIMEOUT, 'WorkerScheduler.update()') as acquired:
                cls.WORKER_QUEUE[worker['worker_id']].update(worker)

        return worker