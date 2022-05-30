from common import create_database, create_table
from constants import *
from DatabaseScheduler import DatabaseScheduler

def setup():

    # Database Scheduler Setup
    create_database(DATABASE_SCHEDULER_DATABASE)

    messages_table_schema = ""
    for k, v in MESSAGES_TABLE_SCHEMA.items():
        messages_table_schema += f"`{k}` {v}, "
    
    messages_table_schema = messages_table_schema[:-2]
    
    create_table(DATABASE_SCHEDULER_RECIPIENT_TABLE, messages_table_schema)

if __name__ == '__main__':
    setup()
    DatabaseScheduler().start()