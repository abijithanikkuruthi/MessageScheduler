import time
from common import create_topics, get_bucket_object_list, printdebug
from constants import SM_TOPIC, SM_TOPIC_PARTITIONS, SM_PARTITIONS_PER_BUCKET, SERVER_HOST, SERVER_PORT, KAFKA_APPLICATION_RESTART_TIME, SM_BUCKETS_MULTIPLICATION_RATIO
from flask import Flask, request
from JobScheduler import JobScheduler
from WorkerScheduler import WorkerScheduler
from ScheduleServer import ScheduleServer

app = Flask(__name__)

def setup():
    while True:
        try:
            # setup scheduled messages topics for incoming messages
            create_topics([{
                'name' : SM_TOPIC,
                'num_partitions' : SM_TOPIC_PARTITIONS,
                'retention' : 2 * KAFKA_APPLICATION_RESTART_TIME
            }])

            # Setup topics for scheduled messages buckets
            create_topics([ { 
                'name' : i['name'],
                'num_partitions' : SM_PARTITIONS_PER_BUCKET, 
                'retention' : (SM_BUCKETS_MULTIPLICATION_RATIO * i['lower'])
            } for i in get_bucket_object_list() ])
            return True
        except Exception as e:
            printdebug(f'Setup Error: {e}')
            time.sleep(1)

@app.route('/', methods=['GET'])
def req_index():
    return ScheduleServer.req_index(request)

@app.route('/config', methods=['GET'])
def req_config():
    return ScheduleServer.req_config(request)

@app.route('/worker/<worker_id>', methods=['GET', 'POST'])
def req_worker(worker_id):
    return ScheduleServer.req_worker(request, worker_id)

@app.route('/api/jq', methods=['GET'])
def api_jq():
    return ScheduleServer.api_jq(request)

@app.route('/api/wq', methods=['GET'])
def api_wq():
    return ScheduleServer.api_wq(request)

@app.route('/api/job_log', methods=['GET'])
def api_job_log():
    return ScheduleServer.api_job_log(request)

def start_services():
    
    printdebug('Starting Job Scheduler and Worker Scheduler modules')
    
    js = JobScheduler()
    js.start()
    
    ws = WorkerScheduler()
    ws.start()

    printdebug('JobScheduler and WorkerScheduler init complete')

# Setup and Scheduler Init
setup()
start_services()

if __name__=="__main__":
    app.run(port=SERVER_PORT, debug=False, host=SERVER_HOST)