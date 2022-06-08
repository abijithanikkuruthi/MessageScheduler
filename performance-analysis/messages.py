import os
import pandas as pd

def analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result/'
    os.makedirs(result_path, exist_ok=True)

    message_database_data_path = f'{path}{os.sep}message-database{os.sep}messages.csv'
    database_scheduler_data_path = f'{path}{os.sep}database-scheduler{os.sep}messages.csv'

    message_database_data = pd.read_csv(message_database_data_path)
    database_scheduler_data = pd.read_csv(database_scheduler_data_path)

    