import os
import pandas as pd
import matplotlib.pyplot as plt
from constants import *
from common import printinfo

def __analyse_kafka(data_path, result_path):

    printinfo(f'Analysing Kafka data')

    data = pd.read_csv(data_path)

    prefix = 'kafka_'

    # Clean up the data
    # Pass
    
    # Message Hop Count
    data['__sm_message_hopcount'] = data['__sm_message_hopcount'].fillna(0).astype(int)
    data['__sm_message_hopcount'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_hopcount.csv')

    fig, ax = plt.subplots(figsize=(16, 9))

    data['__sm_message_hopcount'].value_counts().plot.bar(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Hop Count")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_hopcount.pdf', bbox_inches='tight')

    # Message Delay Computations
    data.time = pd.to_datetime(data.time)
    data.__sm_worker_timestamp.fillna(data.__sm_mh_timestamp, inplace=True)
    data.__sm_worker_timestamp = pd.to_datetime(data.__sm_worker_timestamp)
    data['delay'] = (data.time - data.__sm_worker_timestamp).astype('timedelta64[s]').astype(int)
    data['abs_delay'] = abs((data.time - data.__sm_worker_timestamp).astype('timedelta64[s]')).astype(int)

    # Message Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(16, 9))
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data['Maxiumum'] = data.groupby('time_group')['abs_delay'].transform('max')
    data['95_Percentile'] = data.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    data['Mean'] = data.groupby('time_group')['abs_delay'].transform('mean')

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    ax.fill_between(data['time_group'], data['Maxiumum'], data['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax.fill_between(data['time_group'], data['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data['Maxiumum'] > 60))
    ax.title.set_text('Kafka')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')


def __analyse_cassandra(data_path, result_path):

    printinfo(f'Analysing Cassandra data')

    data = pd.read_csv(data_path)

    prefix = 'cassandra_'

    # Clean up the data
    data = data.drop(columns=['sm_msg_id', 'sm_exp_creation_time', 'sm_exp_id', 'topic', 'value'])
    
    # Message Delay Computations
    data.time = pd.to_datetime(data.time)
    data.sm_recieved_time = pd.to_datetime(data.sm_recieved_time)
    data['delay'] = (data.time - data.sm_recieved_time).astype('timedelta64[s]')
    data['abs_delay'] = abs((data.time - data.sm_recieved_time).astype('timedelta64[s]'))

    # Message Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(16, 9))
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data['Maxiumum'] = data.groupby('time_group')['abs_delay'].transform('max')
    data['95_Percentile'] = data.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    data['Mean'] = data.groupby('time_group')['abs_delay'].transform('mean')

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    ax.fill_between(data['time_group'], data['Maxiumum'], data['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax.fill_between(data['time_group'], data['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data['Maxiumum'] > 60))
    ax.title.set_text('Cassandra')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')

def __analyse_mysql(data_path, result_path):

    printinfo(f'Analysing MySQL data')

    data = pd.read_csv(data_path)

    prefix = 'mysql_'

    # Clean up the data
    data.rename(columns={'0': '__sm_msg_id', '1': 'topic', '2': 'time', '3': 'value', '4': '__sm_exp_id', '5': '__sm_creation_time', '6': '__sm_recieved_time'}, inplace=True)
    data = data.drop(columns=[ '__sm_exp_id', '__sm_msg_id', 'topic', '__sm_creation_time'])
    
    # Message Delay Computations
    data.time = pd.to_datetime(data.time)
    data.__sm_recieved_time = pd.to_datetime(data.__sm_recieved_time)
    data['delay'] = (data.time - data.__sm_recieved_time).astype('timedelta64[s]')
    data['abs_delay'] = abs((data.time - data.__sm_recieved_time).astype('timedelta64[s]'))

    # Message Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(16, 9))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(16, 9))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(16, 9))
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data['Maxiumum'] = data.groupby('time_group')['abs_delay'].transform('max')
    data['95_Percentile'] = data.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    data['Mean'] = data.groupby('time_group')['abs_delay'].transform('mean')

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    ax.fill_between(data['time_group'], data['Maxiumum'], data['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax.fill_between(data['time_group'], data['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data['Maxiumum'] > 60))
    ax.title.set_text('MySQL')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')

def analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result/'
    os.makedirs(result_path, exist_ok=True)

    KAFKA_ENABLED and __analyse_kafka(f'{path}{os.sep}kafka{os.sep}messages.csv', result_path)
    DATABASE_SCHEDULER_CASSANDRA_ENABLED and __analyse_cassandra(f'{path}{os.sep}cassandra{os.sep}messages.csv', result_path)
    DATABASE_SCHEDULER_MYSQL_ENABLED and __analyse_mysql(f'{path}{os.sep}mysql{os.sep}messages.csv', result_path)

    plt.close('all')

def old_analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result/'
    os.makedirs(result_path, exist_ok=True)

    # Read data
    message_database_data_path = f'{path}{os.sep}message-database{os.sep}messages.csv'
    database_scheduler_data_path = f'{path}{os.sep}database-scheduler{os.sep}messages.csv'

    md_df = pd.read_csv(message_database_data_path)
    ds_df = pd.read_csv(database_scheduler_data_path)

    # Clean data
    md_df = md_df.drop(columns=['_id', '__sm_exp_id', '__sm_msg_id', 'topic', '__sm_job_id'])
    ds_df = ds_df.drop(columns=['sm_msg_id', 'sm_exp_id', 'topic', 'value'])
    
    # Message Hop Count
    md_df['__sm_message_hopcount'] = md_df['__sm_message_hopcount'].fillna(0).astype(int)
    md_df['__sm_message_hopcount'].describe().to_csv(f'{result_path}/messages_hopcount.csv')

    fig, ax = plt.subplots(figsize=(16, 9))

    md_df['__sm_message_hopcount'].value_counts().plot.bar(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Hop Count")
    plt.savefig(f'{result_path}/kafka_messages_hopcount.pdf', bbox_inches='tight')

    # Message Delay Computations
    md_df.time = pd.to_datetime(md_df.time)
    md_df.__sm_worker_timestamp.fillna(md_df.__sm_mh_timestamp, inplace=True)
    md_df.__sm_worker_timestamp = pd.to_datetime(md_df.__sm_worker_timestamp)
    md_df['delay'] = (md_df.time - md_df.__sm_worker_timestamp).astype('timedelta64[s]')
    md_df['abs_delay'] = abs((md_df.time - md_df.__sm_worker_timestamp).astype('timedelta64[s]'))

    ds_df.time = pd.to_datetime(ds_df.time)
    ds_df.sm_recieved_time = pd.to_datetime(ds_df.sm_recieved_time)
    ds_df['delay'] = (ds_df.time - ds_df.sm_recieved_time).astype('timedelta64[s]')
    ds_df['abs_delay'] = abs((ds_df.time - ds_df.sm_recieved_time).astype('timedelta64[s]'))

    # Message Delay Histogram
    fig, ax = plt.subplots(figsize=(25, 9))
    hist_df = pd.DataFrame()
    hist_df['Kafka'] = md_df.delay
    hist_df['Cassandra'] = ds_df.delay
    axarr = hist_df.hist(bins=60, ax=ax, sharey=True, sharex=True, legend=False, ylabelsize=12, xlabelsize=12, figsize=(25, 9))
    for ax in axarr.flatten():
        ax.set_xlabel("Message Delivery Delay (s)")
        ax.set_ylabel("Message Count")
    fig.savefig(f'{result_path}/message_delay_comparison.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(25, 9))
    hist_df = pd.DataFrame()
    hist_df['Kafka'] = md_df.abs_delay
    hist_df['Cassandra'] = ds_df.abs_delay
    axarr = hist_df.hist(bins=60, ax=ax, sharey=True, sharex=True, legend=False, ylabelsize=12, xlabelsize=12, figsize=(25, 9))
    for ax in axarr.flatten():
        ax.set_xlabel("Absolute difference between scheduled time and delivered time (s)")
        ax.set_ylabel("Message Count")
    fig.savefig(f'{result_path}/message_abs_delay_comparison.pdf', bbox_inches='tight')

    # Message Delay Statistics
    md_df.delay.describe().to_csv(f'{result_path}/kafka_messages_delay.csv')
    md_df.abs_delay.describe().to_csv(f'{result_path}/kafka_messages_abs_delay.csv')
    ds_df.delay.describe().to_csv(f'{result_path}/cassandra_messages_delay.csv')
    ds_df.abs_delay.describe().to_csv(f'{result_path}/cassandra_messages_abs_delay.csv')

    # Message Delay Overtime
    plt.rcParams['font.size'] = '16'

    fig, ax = plt.subplots(nrows=1, ncols=2, sharex=True, sharey=True, figsize=(25,9), constrained_layout=True)

    md_df = md_df.sort_values(by='time')
    ds_df = ds_df.sort_values(by='time')

    md_df['time_group'] = ((md_df['time'] - md_df['time'].min()).astype('timedelta64[s]')/60).astype(int)

    md_df['Maxiumum'] = md_df.groupby('time_group')['abs_delay'].transform('max')
    md_df['95_Percentile'] = md_df.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    md_df['Mean'] = md_df.groupby('time_group')['abs_delay'].transform('mean')

    md_df['time_group'] = md_df['time_group']/60
    md_df[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax[0], label='Message Database')
    ax[0].fill_between(md_df['time_group'], md_df['Maxiumum'], md_df['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax[0].fill_between(md_df['time_group'], md_df['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(md_df['Maxiumum'] > 60))
    ax[0].title.set_text('Kafka')
    ax[0].set_xlabel('Time (hours)')
    ax[0].set_ylabel('Message Delay (s)')

    ds_df['time_group'] = ((ds_df['time'] - ds_df['time'].min()).astype('timedelta64[s]')/60).astype(int)

    ds_df['Maxiumum'] = ds_df.groupby('time_group')['abs_delay'].transform('max')
    ds_df['95_Percentile'] = ds_df.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    ds_df['Mean'] = ds_df.groupby('time_group')['abs_delay'].transform('mean')

    ds_df['time_group'] = ds_df['time_group']/60
    ds_df[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax[1], label='Database Scheduler')
    ax[1].fill_between(ds_df['time_group'], ds_df['Maxiumum'], ds_df['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax[1].fill_between(ds_df['time_group'], ds_df['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(ds_df['Maxiumum'] > 60))
    ax[1].title.set_text('Cassandra')
    ax[1].set_xlabel('Time (hours)')
    ax[1].set_ylabel('Message Delay (s)')

    fig.savefig(f'{result_path}/message_delay_overtime.pdf', bbox_inches='tight')

    plt.close('all')
