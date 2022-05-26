from flask import make_response
from JobScheduler import JobScheduler, Job
from WorkerScheduler import WorkerScheduler
from constants import *
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
            'sm_consumer_group_prefix' :            SM_CONSUMER_GROUP_PREFIX,
            'sm_time_format' :                      SM_TIME_FORMAT,
            'sm_miniumum_delay' :                   SM_MINIUMUM_DELAY,
            'sm_maximum_delay' :                    SM_MAXIUMUM_DELAY,
            'sm_buckets_multiplication_ratio' :     SM_BUCKETS_MULTIPLICATION_RATIO,
            'sm_partitions_per_bucket' :            SM_PARTITIONS_PER_BUCKET,
            'sm_bucket_topic_format' :              SM_BUCKET_TOPIC_FORMAT,
            'sm_mh_process_count' :                 SM_MH_PROCESS_COUNT,
            'sm_mh_consumer_group_id' :             SM_MH_CONSUMER_GROUP_ID,
            'sm_bucket_process_time_fraction' :     SM_BUCKET_PROCESS_TIME_FRACTION,
            'sm_header_mh_timestamp_key' :          SM_HEADER_MH_TIMESTAMP_KEY,

            'worker_status_list.ready' :            WORKER_STATUS_LIST.READY,
            'worker_status_list.working' :          WORKER_STATUS_LIST.WORKING,
            'worker_status_list.done' :             WORKER_STATUS_LIST.DONE,
            'worker_status_list.error' :            WORKER_STATUS_LIST.ERROR,
            'worker_ready_poll_freq' :              WORKER_READY_POLL_FREQ,
            'worker_working_poll_freq' :            WORKER_WORKING_POLL_FREQ,
            'worker_consumer_timeout' :             WORKER_CONSUMER_TIMEOUT,
            'sm_header_job_id_key' :                SM_HEADER_JOB_ID_KEY,
            'sm_header_worker_timestamp_key' :      SM_HEADER_WORKER_TIMESTAMP_KEY,
            'worker_process_count' :                WORKER_PROCESS_COUNT,
            'sm_header_message_hopcount_key' :      SM_HEADER_MESSAGE_HOPCOUNT_KEY,
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
    
    @classmethod
    def api_job_log(cls, request):
        response = make_response(Job.get_job_log(), 200)
        response.mimetype = "text/plain"
        return response