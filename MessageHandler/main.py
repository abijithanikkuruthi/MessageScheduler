import threading
import multiprocessing
from flask import Flask
from common import get_bucket_list, printerror, printinfo, create_topics, get_config
from config import SM_PARTITIONS_PER_BUCKET, SM_TOPIC, SM_TOPIC_PARTITIONS, CS_HOST, CS_PORT
from MessageHandler import MessageHandler

class MessageHandlerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.mh = MessageHandler()
        self.config = get_config()

    def run(self):
        self.mh.run(self.config)

app = Flask(__name__)

@app.route('/config')
def request_config():
    return get_config()

def config_server():
    try:
        app.run(host=CS_HOST, port=CS_PORT)
    except Exception as e:
        printerror(f'Config Server: {e}')

def setup():
    
    # setup scheduled messages topics for incoming messages
    create_topics([{
        'name' : SM_TOPIC,
        'num_partitions' : SM_TOPIC_PARTITIONS
    }])

    # Setup topics for scheduled messages buckets
    create_topics([ { 'name' : i, 'num_partitions' : SM_PARTITIONS_PER_BUCKET } for i in get_bucket_list() ])

if __name__ == "__main__":
    
    printinfo("MessageScheduler Service Initialization")

    # Initial setup
    setup()

    # Start MessageHandlerThread
    MessageHandlerThreadList = []
    for i in range(multiprocessing.cpu_count()):
        mh_thread = MessageHandlerThread()
        mh_thread.start()
        MessageHandlerThreadList.append(mh_thread)

    # Start config service server
    config_server()