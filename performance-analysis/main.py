import os
import shutil
import pandas as pd
from pymongo import MongoClient
import mysql.connector

from common import get_config
import monitoring

def collect(config):
    path = config['data_path']

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

        cnx = mysql.connector.connect(user=config['database-scheduler']['user'], 
                                password=config['database-scheduler']['password'],
                                host=config['database-scheduler']['host'],
                                database=config['database-scheduler']['database'])
        cursor = cnx.cursor()
        cursor.execute(f"SELECT * FROM {config['database-scheduler']['table']}")
        pd.DataFrame(cursor.fetchall()).to_csv(f'{database_scheduler_data_path}{os.sep}messages.csv')
        cursor.close()
        cnx.close()

def analyse(config):

    monitoring.analyse(config)

    

if __name__ == '__main__':
    config = get_config()

    collect(config)

    # print(config)
    
    # config = {'data_path': '/mnt/Media Drive/Academics/Thesis/MessageScheduler/data/2022-06-10 12-20-53', 'monitoring': {'docker': '/mnt/Media Drive/Academics/Thesis/MessageScheduler/monitoring/docker-monitor/logs', 'prometheus': '/mnt/Media Drive/Academics/Thesis/MessageScheduler/monitoring/prometheus'}, 'message-database': {'enabled': True, 'url': 'mongodb://admin:kafka@localhost:27017/', 'database': 'MESSAGES', 'table': 'MESSAGES_RECIEVED'}, 'database-scheduler': {'enabled': True, 'host': 'localhost', 'user': 'root', 'password': 'kafka', 'database': 'MESSAGES', 'table': 'MESSAGES_RECIEVED'}}

    analyse(config)