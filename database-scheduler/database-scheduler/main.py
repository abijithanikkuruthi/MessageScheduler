from common import create_keyspace, create_table
from constants import *
from DatabaseScheduler import DatabaseScheduler

def setup():

    # Database Scheduler Setup
    create_keyspace(DATABASE_SCHEDULER_KEYSPACE)

    messages_table_schema = ""
    for k, v in MESSAGES_TABLE_SCHEMA.items():
        messages_table_schema += f"{k.replace('__', '')} {v}, "
    
    messages_table_schema += f" PRIMARY KEY ({MESSAGE_ID_KEY.replace('__', '')}, time)"
    
    create_table(DATABASE_SCHEDULER_KEYSPACE, DATABASE_SCHEDULER_RECIPIENT_TABLE, messages_table_schema)

if __name__ == '__main__':
    setup()
    DatabaseScheduler().start()