from Messenger import Messenger
from Collector import Collector
from common import create_topics
from constants import *

def setup():
    create_topics([{
        'name' : KAFKA_MESSAGE_TOPIC,
        'num_partitions' : KAFKA_MESSAGE_TOPIC_PARTITIONS,
        'retention' : EXPERIMENT_DURATION_HOURS * 60 * 60
    }])

if __name__ == '__main__':
    setup()
    Messenger().start()
    Collector().start()