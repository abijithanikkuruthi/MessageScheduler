from common import create_topics, get_bucket_list
from config import SM_TOPIC, SM_TOPIC_PARTITIONS, SM_PARTITIONS_PER_BUCKET

def setup():
    # setup scheduled messages topics for incoming messages
    create_topics([{
        'name' : SM_TOPIC,
        'num_partitions' : SM_TOPIC_PARTITIONS
    }])

    # Setup topics for scheduled messages buckets
    create_topics([ { 'name' : i, 'num_partitions' : SM_PARTITIONS_PER_BUCKET } for i in get_bucket_list() ])

# Setup and Scheduler Init
setup()


if __name__=='__main__':

    # Flask server startup code
    pass
