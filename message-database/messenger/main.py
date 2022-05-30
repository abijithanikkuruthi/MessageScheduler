from Messenger import Messenger
from Collector import Collector
from common import create_topics, create_database, create_table
from constants import *

def setup():
    # Kafka Setup
    create_topics([{
        'name' : KAFKA_MESSAGE_TOPIC,
        'num_partitions' : KAFKA_MESSAGE_TOPIC_PARTITIONS,
        'retention' : EXPERIMENT_DURATION_HOURS * 60 * 60
    }])

    # Database Scheduler Setup
    create_database(DATABASE_SCHEDULER_DATABASE)

    messages_table_schema = ""
    for k, v in MESSAGES_TABLE_SCHEMA.items():
        messages_table_schema += f"`{k}` {v}, "
    
    messages_table_schema = messages_table_schema[:-2]
    
    create_table(DATABASE_SCHEDULER_SM_TABLE, messages_table_schema)

if __name__ == '__main__':
    setup()
    Messenger().start()
    Collector().start()