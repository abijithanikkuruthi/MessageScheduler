import os
import shutil
import pandas as pd
from pymongo import MongoClient
from cassandra.cluster import Cluster
import mysql.connector
import time
import datetime
import sys

from common import get_config, make_config, printinfo, printsuccess, printwarning, printerror, ProgressInfo
import ServiceAnalyser
import MessageAnalyser
from constants import *
import constants

def collect(config):
    path = config['data_path']

    # Collect monitoring data from docker containers
    try:
        docker_data_path = f'{path}{os.sep}docker{os.sep}'
        shutil.copytree(config['monitoring']['docker'], docker_data_path)
    except:
        printwarning('Could not collect Docker data')
        
    # Collect kafka monitoring data and messages
    if KAFKA_ENABLED:
        try:
            prometheus_data_path = f'{path}{os.sep}prometheus{os.sep}'
            shutil.copytree(config['monitoring']['prometheus'], prometheus_data_path)
        except:
            printwarning('Could not collect Prometheus data')
        
        kafka_data_path = f'{path}{os.sep}kafka{os.sep}'
        os.makedirs(kafka_data_path, exist_ok=True)

        try:
            message_database = MongoClient(MESSENGER_DATABASE_URL)[MESSENGER_DATABASE_NAME][MESSENGER_DATABASE_TABLE]
            msg_count = message_database.count_documents({})
            kafka_progress = ProgressInfo('Kafka data collection', msg_count)
            output_path = f'{kafka_data_path}{os.sep}messages.csv'
            os.remove(output_path) if os.path.exists(output_path) else None

            printinfo(f'Collecting {msg_count} messages from Kafka')

            for i in range(0, msg_count, KAFKA_BATCH_SIZE):
                messages = message_database.find({}, { 'time': 1, '__sm_mh_timestamp': 1, '__sm_worker_timestamp': 1, '__sm_message_hopcount' : 1 }).skip(i).limit(KAFKA_BATCH_SIZE)
                df = pd.DataFrame(messages, columns=['time', '__sm_mh_timestamp', '__sm_worker_timestamp', '__sm_message_hopcount'])
                df.to_csv(output_path, mode='a', header=not os.path.exists(output_path), index=False)
                kafka_progress.update(i)

        except Exception as e:
            printwarning('Could not collect Kafka data : ' + str(e))
            constants.KAFKA_ENABLED = False
    
    # Collect Cassandra Data
    if DATABASE_SCHEDULER_CASSANDRA_ENABLED:
        try:
            printinfo('Collecting Cassandra data')
            cassandra_data_path = f'{path}{os.sep}cassandra{os.sep}'
            os.makedirs(cassandra_data_path, exist_ok=True)

            cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
            session = cluster.connect()
            session.set_keyspace(DATABASE_SCHEDULER_DATABASE)
            
            result = session.execute(f"SELECT * FROM {DATABASE_SCHEDULER_RECIPIENT_TABLE}")
            pd.DataFrame(result).to_csv(f'{cassandra_data_path}{os.sep}messages.csv')
            
            cluster.shutdown()
        except Exception as e:
            printerror('Could not collect Cassandra data : ' + str(e))
            constants.DATABASE_SCHEDULER_CASSANDRA_ENABLED = False

    
    # Collect MySQL Data
    if DATABASE_SCHEDULER_MYSQL_ENABLED:
        try:
            printinfo('Collecting MySQL data')
            mysql_data_path = f'{path}{os.sep}mysql{os.sep}'
            os.makedirs(mysql_data_path, exist_ok=True)

            connection = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER,
                                                    password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                                                    host=DATABASE_SCHEDULER_MYSQL_HOST,
                                                    database=DATABASE_SCHEDULER_DATABASE)
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {DATABASE_SCHEDULER_RECIPIENT_TABLE}")
            pd.DataFrame(cursor.fetchall()).to_csv(f'{mysql_data_path}{os.sep}messages.csv')
            connection.close()
        except Exception as e:
            printerror('Could not collect MySQL data : ' + str(e))
            constants.DATABASE_SCHEDULER_MYSQL_ENABLED = False

def experiment_running(print_log=False):
    def __mongo_running():
        if KAFKA_ENABLED:
            try:
                message_database = MongoClient(MESSENGER_DATABASE_URL)[MESSENGER_DATABASE_NAME][MESSENGER_DATABASE_TABLE]
                msg_count = message_database.count_documents({})
                running = msg_count < EXPERIMENT_MESSAGE_COUNT
                print_log and running and printwarning(f'Kafka is missing {EXPERIMENT_MESSAGE_COUNT - msg_count} messages')
                (not running) and printsuccess('Kafka has finished')
                return running
            except Exception as e:
                print_log and printerror('Could not connect to MongoDB : ' + str(e))
                return True
        return False

    def __cassandra_running():
        if DATABASE_SCHEDULER_CASSANDRA_ENABLED:
            try:
                cluster = Cluster([DATABASE_SCHEDULER_CASSANDRA_HOST])
                session = cluster.connect()
                session.set_keyspace(DATABASE_SCHEDULER_DATABASE)
                result = session.execute(f"SELECT COUNT(*) FROM {DATABASE_SCHEDULER_RECIPIENT_TABLE}")
                cluster.shutdown()
                running = result.one()[0] < EXPERIMENT_MESSAGE_COUNT
                print_log and running and printwarning(f'Cassandra is missing {EXPERIMENT_MESSAGE_COUNT - result.one()[0]} messages')
                (not running) and printsuccess('Cassandra has finished')
                return running
            except Exception as e:
                print_log and printerror('Could not connect to Cassandra : ' + str(e))
                return True
        return False
    
    def __mysql_running():
        if DATABASE_SCHEDULER_MYSQL_ENABLED:
            try:
                connection = mysql.connector.connect(user=DATABASE_SCHEDULER_MYSQL_USER, 
                                password=DATABASE_SCHEDULER_MYSQL_PASSWORD,
                              host=DATABASE_SCHEDULER_MYSQL_HOST,
                              database=DATABASE_SCHEDULER_DATABASE)
                cursor = connection.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {DATABASE_SCHEDULER_RECIPIENT_TABLE}")
                result = cursor.fetchone()
                connection.close()
                running = result[0] < EXPERIMENT_MESSAGE_COUNT
                print_log and running and printwarning(f'MySQL is missing {EXPERIMENT_MESSAGE_COUNT - result[0]} messages')
                (not running) and printsuccess('MySQL has finished')
                return running
            except Exception as e:
                print_log and printerror('Could not connect to MySQL : ' + str(e))
                return True
        return False
    mongo_running = __mongo_running()
    cassandra_running = __cassandra_running()
    mysql_running = __mysql_running()
    return mongo_running or cassandra_running or mysql_running

def analyse(config):
    ServiceAnalyser.analyse(config)
    MessageAnalyser.analyse(config)

if __name__ == '__main__':

    if len(sys.argv) == 1:
        config = get_config()

        # Save experiment environment file in data folder
        shutil.copyfile(f'{config["root_path"]}{os.sep}experiment.env', f'{config["data_path"]}{os.sep}experiment.env')

        EXPERIMENT_DURATION_HOURS = (EXPERIMENT_DURATION_HOURS + 1) if EXPERIMENT_DURATION_HOURS > 1 else EXPERIMENT_DURATION_HOURS
        
        printinfo(f'Waiting for experiment to finish. Expected completion time: {(datetime.datetime.now() + datetime.timedelta(hours=EXPERIMENT_DURATION_HOURS)).strftime(TIME_FORMAT)}')

        experiment_running() and time.sleep(EXPERIMENT_DURATION_HOURS * 60 * 60)

        retries = 0
        while experiment_running(print_log=True):
            printwarning(f'All messages are yet to be collected... Waiting for 1 more minute...')
            time.sleep(60)
            retries += 1
            if retries > 10:
                printerror(f'Experiment is still running after {retries} minutes')
                break
        
        printsuccess('Collecting data...')
        collect(config)
        printsuccess('Data collected! Analyzing data...')
    
    elif len(sys.argv) == 2:
        config = make_config(sys.argv[1])
        printinfo(f'Analyzing {config["data_path"]}')
    
    else:
        printerror('Invalid number of arguments')
        sys.exit(1)
        
    analyse(config)
    printsuccess('Data analyzed!')
    printsuccess(f'Data is stored in {config["data_path"]}')
    printsuccess('Experiment Finished! You can now safely stop the containers using "--stop" parameter')
