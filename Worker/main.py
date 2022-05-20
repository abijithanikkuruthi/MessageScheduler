import time
from common import Config, printdebug, printsuccess, printerror, printheader, WorkerHandler
from constants import SCHEDULER_SERVER_URL, REQUEST_ERROR_WAIT_TIME, REQUEST_COUNT_LIMIT
from Worker import Worker

if __name__ == "__main__":
    
    printheader('Worker started')

    while True:

        work_object = WorkerHandler.get_worker()
        
        # READY state - Wait for a task
        if work_object['status'] == Config.get('worker_status_list')[0]:
            
            work_object = WorkerHandler.post_worker(work_object)
            printdebug(f'Work: {work_object}')
            
            work_object['status'] == Config.get('worker_status_list')[1] and Worker(work_object).start()


        # WORKING state - do nothing
        elif work_object['status'] == Config.get('worker_status_list')[1]:
            printdebug(f'Worker is working on {work_object}')


        # DONE state - Change to READY state and wait for a task
        # ERROR state - Change to READY state and wait for a task
        elif (work_object['status'] == Config.get('worker_status_list')[2]) or (work_object['status'] == Config.get('worker_status_list')[-1]):
            printdebug(f'Worker is done with {work_object}')

            work_object['status'] = Config.get('worker_status_list')[0]
            
            work_object = WorkerHandler.post_worker(work_object)
            printdebug(f'Work: {work_object}')
            
            work_object['status'] == Config.get('worker_status_list')[1] and Worker(work_object).start()

        sleep_time = Config.get('worker_ready_poll_freq') if work_object['status'] == Config.get('worker_status_list')[0] else Config.get('worker_working_poll_freq')
        printdebug(f'Sleeping for {sleep_time} seconds')
        time.sleep(sleep_time)
