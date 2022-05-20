from JobScheduler import JobScheduler
from WorkerScheduler import WorkerScheduler
from config import SM_MAXIUMUM_DELAY, SM_MINIUMUM_DELAY, SM_TIME_FORMAT, SM_TOPIC, SM_TOPIC_PARTITIONS, SM_PARTITIONS_PER_BUCKET, SERVER_HOST, SERVER_PORT, KAFKA_SERVER, SM_CONSUMER_GROUP_NAME, SM_BUCKETS_MULTIPLICATION_RATIO, SM_BUCKET_TOPIC_FORMAT, SM_MH_THREAD_COUNT
from common import id_generator, get_bucket_list, get_bucket_object_list

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
            'sm_topic' : SM_TOPIC,
            'sm_topic_partitions' : SM_TOPIC_PARTITIONS,
            'sm_partitions_per_bucket' : SM_PARTITIONS_PER_BUCKET,
            'bucket_list' : get_bucket_list(),
            'bucket_object_list' : get_bucket_object_list(),
            'kafka_server' : KAFKA_SERVER,
            'sm_consumer_group_name' : SM_CONSUMER_GROUP_NAME,
            'sm_time_format' : SM_TIME_FORMAT,
            'sm_miniumum_delay' : SM_MINIUMUM_DELAY,
            'sm_maximum_delay' : SM_MAXIUMUM_DELAY,
            'sm_buckets_multiplication_ratio' : SM_BUCKETS_MULTIPLICATION_RATIO,
            'sm_partitions_per_bucket' : SM_PARTITIONS_PER_BUCKET,
            'sm_bucket_topic_format' : SM_BUCKET_TOPIC_FORMAT,
            'sm_mh_thread_count' : SM_MH_THREAD_COUNT,
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