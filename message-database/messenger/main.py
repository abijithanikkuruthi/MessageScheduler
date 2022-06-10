from Messenger import Messenger
from Collector import Collector
from common import create_topics, create_keyspace, create_table
from constants import *

def setup():

    if KAFKA_ENABLED:
        # Kafka Setup
        create_topics([{
            'name' : KAFKA_MESSAGE_TOPIC,
            'num_partitions' : KAFKA_MESSAGE_TOPIC_PARTITIONS,
            'retention' : EXPERIMENT_DURATION_HOURS * 60 * 60
        }])

    if DATABASE_SCHEDULER_ENABLED:
        create_keyspace(DATABASE_SCHEDULER_KEYSPACE)

        messages_table_schema = ""
        for k, v in MESSAGES_TABLE_SCHEMA.items():
            messages_table_schema += f"{k.replace('__', '')} {v}, "
        
        messages_table_schema += f" PRIMARY KEY ({MESSAGE_ID_KEY.replace('__', '')}, time)"
        
        create_table(DATABASE_SCHEDULER_KEYSPACE, DATABASE_SCHEDULER_SM_TABLE, messages_table_schema)

if __name__ == '__main__':
    setup()
    Messenger().start()
    Collector().start()