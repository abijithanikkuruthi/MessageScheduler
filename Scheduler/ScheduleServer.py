from JobScheduler import JobScheduler
from WorkerScheduler import WorkerScheduler
from config import *
from common import get_bucket_list, get_bucket_object_list, printerror
import json

# HTTP Server for Schedule Job Handling
class ScheduleServer:
    def __init__(self):
        pass

    @classmethod
    def req_index(cls, request):
        return {
            'status' :  'OK',
            'message' : 'ScheduleServer is running',
        }

    @classmethod
    def req_config(cls, request):
        return {
            'sm_topic' :                            SM_TOPIC,
            'sm_topic_partitions' :                 SM_TOPIC_PARTITIONS,
            'sm_partitions_per_bucket' :            SM_PARTITIONS_PER_BUCKET,
            'bucket_list' :                         get_bucket_list(),
            'bucket_object_list' :                  get_bucket_object_list(),
            'kafka_server' :                        KAFKA_SERVER,
            'sm_consumer_group_name' :              SM_CONSUMER_GROUP_NAME,
            'sm_time_format' :                      SM_TIME_FORMAT,
            'sm_miniumum_delay' :                   SM_MINIUMUM_DELAY,
            'sm_maximum_delay' :                    SM_MAXIUMUM_DELAY,
            'sm_buckets_multiplication_ratio' :     SM_BUCKETS_MULTIPLICATION_RATIO,
            'sm_partitions_per_bucket' :            SM_PARTITIONS_PER_BUCKET,
            'sm_bucket_topic_format' :              SM_BUCKET_TOPIC_FORMAT,
            'sm_mh_thread_count' :                  SM_MH_THREAD_COUNT,
            'sm_mh_process_count' :                 SM_MH_PROCESS_COUNT,

            'worker_status_list' :                  WORKER_STATUS_LIST,
            'worker_thread_count' :                 WORKER_THREAD_COUNT,
            'worker_ready_poll_freq' :              WORKER_READY_POLL_FREQ,
            'worker_working_poll_freq' :            WORKER_WORKING_POLL_FREQ,
            'worker_consumer_timeout_ms' :          WORKER_CONSUMER_TIMEOUT_MS,
            'sm_header_job_id_key' :                SM_HEADER_JOB_ID_KEY,
            'worker_process_count' :                WORKER_PROCESS_COUNT,
        }

    @classmethod
    def req_worker(cls, request, worker_id):
        if request.method == 'POST':
            try:
                data = json.loads(request.data)
                data['ip'] = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR', 'unknown'))
                
                return WorkerScheduler.update(data)
            except Exception as e:
                printerror(f'{e}')
                return {
                    'status' :  'ERROR',
                    'message' : 'Worker status update failed',
                }
        else:
            return WorkerScheduler.get_worker({
                'worker_id' : worker_id,
                'ip'        : request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR', 'unknown'))
            })
    
    @classmethod
    def api_jq(cls, request):
        return {
            'job_queue' : JobScheduler.get_job_queue()
        }

    @classmethod
    def api_wq(cls, request):
        return {
            'worker_queue' : WorkerScheduler.get_worker_queue()
        }