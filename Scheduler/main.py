from common import create_topics, get_bucket_list, printdebug
from config import SM_TOPIC, SM_TOPIC_PARTITIONS, SM_PARTITIONS_PER_BUCKET, SERVER_HOST, SERVER_PORT
from flask import Flask, request
from JobScheduler import JobScheduler
from WorkerScheduler import WorkerScheduler
from ScheduleServer import ScheduleServer

app = Flask(__name__)

def setup():
    # setup scheduled messages topics for incoming messages
    create_topics([{
        'name' : SM_TOPIC,
        'num_partitions' : SM_TOPIC_PARTITIONS
    }])

    # Setup topics for scheduled messages buckets
    create_topics([ { 'name' : i, 'num_partitions' : SM_PARTITIONS_PER_BUCKET } for i in get_bucket_list() ])

@app.route('/', methods=['GET'])
def req_index():
    return ScheduleServer.req_index(request)

@app.route('/config', methods=['GET'])
def req_config():
    return ScheduleServer.req_config(request)

@app.route('/worker', methods=['GET', 'POST'])
def req_worker():
    return ScheduleServer.req_worker(request)

@app.route('/api/jq', methods=['GET'])
def api_jq():
    return ScheduleServer.api_jq(request)

@app.route('/api/wq', methods=['GET'])
def api_wq():
    return ScheduleServer.api_wq(request)

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