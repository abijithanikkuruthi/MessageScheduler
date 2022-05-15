from common import get_bucket_list, printinfo, create_topics
from config import SM_PARTITIONS_PER_BUCKET, SM_TOPIC, SM_TOPIC_PARTITIONS

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
