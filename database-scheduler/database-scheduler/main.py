from common import create_cassandra_keyspace, create_cassandra_table, create_mysql_database, create_mysql_table
from constants import *
from DatabaseScheduler import CassandraDatabaseScheduler, MySQLDatabaseScheduler

def setup():
    
    # MySQL Setup - Create Database and Recipient Table
    if DATABASE_SCHEDULER_MYSQL_ENABLED:
        create_mysql_database(DATABASE_SCHEDULER_DATABASE)

        messages_table_schema = ""
        for k, v in MESSAGES_TABLE_SCHEMA_MYSQL.items():
            messages_table_schema += f"`{k}` {v}, "
        
        messages_table_schema = messages_table_schema[:-2]
        
        create_mysql_table(DATABASE_SCHEDULER_DATABASE, DATABASE_SCHEDULER_RECIPIENT_TABLE, messages_table_schema)
    
    # Cassandra Setup - Create Keyspace and Recipient Table
    if DATABASE_SCHEDULER_CASSANDRA_ENABLED:
        create_cassandra_keyspace(DATABASE_SCHEDULER_DATABASE)

        messages_table_schema = ""
        for k, v in MESSAGES_TABLE_SCHEMA_CASSANDRA.items():
            # Cassandra does not allow column names to start with underscores
            messages_table_schema += f"{k.replace('__', '')} {v}, "
        
        messages_table_schema += f" PRIMARY KEY ({MESSAGE_ID_KEY.replace('__', '')}, time)"
        
        create_cassandra_table(DATABASE_SCHEDULER_DATABASE, DATABASE_SCHEDULER_RECIPIENT_TABLE, messages_table_schema)

if __name__ == '__main__':

    setup()
    
    DATABASE_SCHEDULER_CASSANDRA_ENABLED and CassandraDatabaseScheduler().start()
    DATABASE_SCHEDULER_MYSQL_ENABLED and MySQLDatabaseScheduler().start()