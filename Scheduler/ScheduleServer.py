from JobScheduler import JobScheduler
from WorkerScheduler import WorkerScheduler
from config import SM_TOPIC, SM_TOPIC_PARTITIONS, SM_PARTITIONS_PER_BUCKET, SERVER_HOST, SERVER_PORT
from common import id_generator

class ScheduleServer:
    def __init__(self):
        pass

    @classmethod
    def req_index(self, request):
        return {
            'status' : 'OK',
            'message' : 'ScheduleServer is running',
        }

    @classmethod
    def req_config(self, request):
        return {
            'status' : 'OK',
            'sm_topic' : SM_TOPIC,
            'sm_topic_partitions' : SM_TOPIC_PARTITIONS,
            'sm_partitions_per_bucket' : SM_PARTITIONS_PER_BUCKET,
        }

    @classmethod
    def req_worker(self, request):
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            ip_addr = request.environ['REMOTE_ADDR']
        else:
            ip_addr = request.environ['HTTP_X_FORWARDED_FOR']
        
        wkr = {
            'ip_addr' : ip_addr,
            'worker_id' : id_generator(),
            'status': 'READY',
        }

        wkr = WorkerScheduler.update(wkr)
        return wkr
    
    @classmethod
    def api_jq(self, request):
        return {
            'job_queue' : JobScheduler.get_job_queue()
        }

    @classmethod
    def api_wq(self, request):
        return {
            'worker_queue' : WorkerScheduler.get_worker_queue()
        }