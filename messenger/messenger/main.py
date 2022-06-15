from Messenger import Messenger
from Collector import Collector
from common import create_mysql_database, create_mysql_table, create_topics, create_cassandra_keyspace, create_cassandra_table
from constants import *

def setup():

    # Apache Kafka Setup - Create Topics to send messages to with appropriate partitions and retention policies
    if KAFKA_ENABLED:
        create_topics([{
            'name' : KAFKA_MESSAGE_TOPIC,
            'num_partitions' : KAFKA_MESSAGE_TOPIC_PARTITIONS,
            'retention' : 3 * EXPERIMENT_DURATION_HOURS * 60 * 60
        }])

    # Cassandra Setup - Create Keyspace and Table
    if DATABASE_SCHEDULER_CASSANDRA_ENABLED:
        create_cassandra_keyspace(DATABASE_SCHEDULER_DATABASE)

        messages_table_schema = ""
        for k, v in MESSAGES_TABLE_SCHEMA_CASSANDRA.items():
            # Cassandra does not allow column names to start with underscores
            messages_table_schema += f"{k.replace('__', '')} {v}, "
        
        messages_table_schema += f" PRIMARY KEY ({MESSAGE_ID_KEY.replace('__', '')}, time)"
        
        create_cassandra_table(DATABASE_SCHEDULER_DATABASE, DATABASE_SCHEDULER_SM_TABLE, messages_table_schema)
    
    # MySQL Setup - Create Database and Table
    if DATABASE_SCHEDULER_MYSQL_ENABLED:
        create_mysql_database(DATABASE_SCHEDULER_DATABASE)

        messages_table_schema = ""
        for k, v in MESSAGES_TABLE_SCHEMA_MYSQL.items():
            messages_table_schema += f"`{k}` {v}, "
        
        messages_table_schema = messages_table_schema[:-2]
        
        create_mysql_table(DATABASE_SCHEDULER_DATABASE, DATABASE_SCHEDULER_SM_TABLE, messages_table_schema)

if __name__ == '__main__':
    setup()
    Messenger().start()
    Collector().start()