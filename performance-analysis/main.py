import os
import shutil
import pandas as pd

from common import get_config

def monitoring(config, path):
    docker_data_path = f'{path}{os.sep}docker/'
    shutil.copytree(config['docker'], docker_data_path)

    docker_log_path = f'{path}{os.sep}docker/docker-monitor.log'
    scheduler_job_path = f'{path}{os.sep}docker/job.log'

    docker_log_df = pd.read_csv(docker_log_path)
    scheduler_job_df = pd.read_csv(scheduler_job_path)

    print(docker_log_df)
    print(scheduler_job_df)


    prometheus_data_path = f'{path}{os.sep}prometheus/'
    shutil.copytree(config['prometheus'], prometheus_data_path)

if __name__ == '__main__':
    config = get_config()

    monitoring(config['monitoring'], config['data_path'])
    