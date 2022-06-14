import os
import shutil
import pandas as pd
from pymongo import MongoClient
from cassandra.cluster import Cluster
import time

from common import get_config, printinfo, printsuccess, printwarning
import monitoring
import messages

def collect(config):
    path = config['data_path']

    # Collect Docker and monitoring data
    docker_data_path = f'{path}{os.sep}docker/'
    shutil.copytree(config['monitoring']['docker'], docker_data_path)

    prometheus_data_path = f'{path}{os.sep}prometheus/'
    shutil.copytree(config['monitoring']['prometheus'], prometheus_data_path)

    # Message Database Data
    if config['message-database']['enabled']:
        message_database_data_path = f'{path}{os.sep}message-database/'
        os.makedirs(message_database_data_path, exist_ok=True)

        message_database = MongoClient(config['message-database']['url'])[config['message-database']['database']][config['message-database']['table']]
        message_database_data = message_database.find()
        pd.DataFrame(message_database_data).to_csv(f'{message_database_data_path}{os.sep}messages.csv')

    
    # Database Scheduler Data
    if config['database-scheduler']['enabled']:
        database_scheduler_data_path = f'{path}{os.sep}database-scheduler/'
        os.makedirs(database_scheduler_data_path, exist_ok=True)

        cluster = Cluster([config['database-scheduler']['host']])
        session = cluster.connect()
        session.set_keyspace(config['database-scheduler']['database'].lower())
        
        result = session.execute(f"SELECT * FROM {config['database-scheduler']['table']}")
        pd.DataFrame(result).to_csv(f'{database_scheduler_data_path}{os.sep}messages.csv')
        
        cluster.shutdown()

def experiment_running(config):
    def __mongo_running():
        try:
            message_database = MongoClient(config['message-database']['url'])[config['message-database']['database']][config['message-database']['table']]
            return message_database.count_documents({}) < config['message-count']
        except:
            return True

    def __cassandra_running():
        try:
            cluster = Cluster([config['database-scheduler']['host']])
            session = cluster.connect()
            session.set_keyspace(config['database-scheduler']['database'].lower())
            result = session.execute(f"SELECT COUNT(*) FROM {config['database-scheduler']['table'].lower()}")
            cluster.shutdown()
            return result[0][0] < config['message-count']
        except:
            return True

    return __mongo_running() or __cassandra_running()

def analyse(config):
    monitoring.analyse(config)
    messages.analyse(config)

if __name__ == '__main__':

    config = get_config()

    printsuccess('Starting performance analysis')
    printinfo(f'Config: {config}')

    printinfo('Waiting for experiment to finish')

    if experiment_running(config):
        time.sleep(config['duration'])

    while experiment_running(config):
        printwarning(f'Experiment is running... Waiting for 1 minute...')
        time.sleep(60)
    
    printsuccess('Experiment is finished! Collecting data...')
    collect(config)
    printsuccess('Data collected! Analyzing data...')
    analyse(config)
    printsuccess('Data analyzed!')
    printsuccess(f'Data is stored in {config["data_path"]}')
    printsuccess('Finished!')
