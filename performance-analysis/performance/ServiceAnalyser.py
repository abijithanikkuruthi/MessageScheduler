import os
import pandas as pd
import matplotlib.pyplot as plt
from common import printerror, printinfo
from constants import *

def __analyse_job_log(log_path, result_path):

    printinfo(f'Analysing job log file: {log_path}')

    job_df = pd.read_csv(log_path)
    job_df.columns = [i.strip() for i in job_df.columns]

    for i in ['creation_time', 'finish_time']:
        job_df[i] = pd.to_datetime(job_df[i])
    
    job_df['duration'] = (job_df['finish_time'] - job_df['creation_time']).astype('timedelta64[s]')

    job_df['name'] = job_df['name'].str.replace('__SM_', '')
    job_df['creation_time'] = (job_df['creation_time'] - job_df['creation_time'].min()).astype('timedelta64[s]')/3600

    # JOB EXECUTION TIME GRAPH
    fig, ax = plt.subplots(figsize=(16, 9))
    for label, df in job_df.groupby('name'):
        df.plot(x='creation_time', y='duration',ax=ax, label=label)
    ax.set_ylabel("Execution Time (s)")
    ax.set_xlabel("Time (hours)")
    plt.legend()
    plt.savefig(f'{result_path}{os.sep}kafka_scheduler_job_exec_time.pdf', bbox_inches='tight')

    # JOB EXECUTION TIME STATS
    job_trim_df = job_df[['name', 'duration']]
    stats = job_trim_df.describe()
    stats.columns = ["Overall"]

    for i in job_trim_df['name'].unique():
        b_stats = job_trim_df[job_trim_df['name'] == i].describe()
        b_stats.rename(columns={'duration': i}, inplace=True)
        stats = pd.concat([stats, b_stats], axis=1)
    stats = stats.round(decimals=3).transpose()
    stats.to_csv(f'{result_path}{os.sep}kafka_scheduler_job_exec_time_stats.csv')

def __analyse_docker_log(log_path, result_path):

    printinfo(f'Analysing docker log file: {log_path}')
    
    docker_df = pd.read_csv(log_path)

    docker_df.columns = [i.strip() for i in docker_df.columns]

    docker_df.name = docker_df.name.str.strip()
    docker_df['time'] = pd.to_datetime(docker_df['time'])
    docker_df['timestamp'] = docker_df['time'].dt.strftime('%Y-%m-%d %H:%M')
    docker_df['time'] = (docker_df['time'] - docker_df['time'].min()).astype('timedelta64[s]')/3600
    docker_df = docker_df[docker_df['name'].isin(['database-scheduler', 'cassandra', 'mysql','messagehandler', 'worker', 'scheduler', 'kafka'])]

    plot_numbers_list = ['pids', 'net_tx (MB)', 'net_rx (MB)',
        'memory_usage (MB)', 'cpu_user (s)', 'cpu_system (s)',
        'blkio_write (MB)', 'blkio_read (MB)']

    # Absolute Readings
    for col_name in [i for i in docker_df.columns if i not in ["time", "name", "timestamp"]]:
        try:
            fig, ax = plt.subplots(figsize=(16, 9))
            for label, df in docker_df.groupby('name'):
                df.plot(x='time', y=col_name, ax=ax, label=label)
            ax.set_ylabel(col_name)
            ax.set_xlabel("Time (hours)")
            plt.legend()
            plt.savefig(f'{result_path}/docker_{col_name}.pdf', bbox_inches='tight')
        except Exception as e:
            printerror(f'Error in plotting docker monitoring data (absolute): {col_name}\n{e}')


    # Rate of change in values per second
    for col_name in [i for i in docker_df.columns if i not in ["time", "name", "timestamp"]]:
        try:
            fig, ax = plt.subplots(figsize=(16, 9))
            for label, df in docker_df.groupby('name'):
                for p in plot_numbers_list:
                    df[p] = df[p].diff()
                df = df[plot_numbers_list + ['timestamp']].groupby('timestamp').sum()/60
                df['timestamp'] = pd.to_datetime(df.index)
                df['time'] = (df['timestamp'] - df['timestamp'].min()).astype('timedelta64[s]')/3600
                df.plot(x='time', y=col_name, ax=ax, label=label)
            ax.set_ylabel(col_name + " per second")
            ax.set_xlabel("Time (hours)")
            plt.legend()
            plt.savefig(f'{result_path}/docker_{col_name}_per_sec.pdf', bbox_inches='tight')
        except Exception as e:
            printerror(f'Error in plotting docker monitoring data (rate): {col_name}\n{e}')
    
def analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result{os.sep}'
    os.makedirs(result_path, exist_ok=True)

    docker_data_path = f'{path}{os.sep}docker{os.sep}'

    kafka_job_log_path = f'{docker_data_path}{os.sep}{KAFKA_SCHEDULER_JOB_LOG_FILE}'
    docker_log_path = f'{docker_data_path}{os.sep}{DOCKER_MONITOR_LOG_FILE}'

    KAFKA_ENABLED and __analyse_job_log(kafka_job_log_path, result_path)
    __analyse_docker_log(docker_log_path, result_path)
    plt.close('all')

