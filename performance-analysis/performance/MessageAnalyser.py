import os
import pandas as pd
import matplotlib.pyplot as plt
from constants import *
from common import get_absolute_path, printinfo, printwarning, printerror
import gc

def __analyse_kafka(data_path, result_path):

    printinfo(f'Analysing Kafka data')

    # Check if data path exists
    if not os.path.exists(data_path):
        printerror(f'Data path {data_path} does not exist')
        return
    
    data = pd.read_csv(data_path)

    prefix = 'kafka_'

    # Clean up the data
    # Pass
    
    # Message Hop Count
    data['__sm_message_hopcount'] = data['__sm_message_hopcount'].fillna(0).astype(int)
    data['__sm_message_hopcount'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_hopcount.csv')

    fig, ax = plt.subplots(figsize=(8, 4))

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
    plt.close()

    # Message Delay Histogram
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')
    plt.close()

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')
    plt.close()

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(8, 4))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')
    plt.close()

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(8, 4), rasterized=False, dpi=100)
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data_groupby_time_group = data[['time_group', 'abs_delay']].groupby('time_group')['abs_delay']

    data = data[['time_group']]
    data['Maxiumum'] = data_groupby_time_group.transform('max')
    data['95_Percentile'] = data_groupby_time_group.transform('quantile', 0.95)
    data['Mean'] = data_groupby_time_group.transform('mean')
    data['Count'] = data_groupby_time_group.transform('count')

    del data_groupby_time_group
    gc.collect()

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    
    # do fill_between in blocks of 100000
    for i in range(0, len(data), 100000):
        ax.fill_between(data.iloc[i:i+100000]['time_group'], data.iloc[i:i+100000]['Maxiumum'], data.iloc[i:i+100000]['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True, linewidth=0.0)
    
    for i in range(0, len(data), 100000):
        ax.fill_between(data.iloc[i:i+100000]['time_group'], data.iloc[i:i+100000]['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data.iloc[i:i+100000]['Maxiumum'] > 60), linewidth=0.0)
    
    ax.title.set_text('Kafka')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    printinfo(f'Saving {result_path}{os.sep}{prefix}messages_delay_over_time.jpg')

    gc.collect()

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.jpg', bbox_inches='tight')
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.png', bbox_inches='tight')
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')
    plt.close()

    # Message count over time
    fig, ax = plt.subplots(figsize=(8, 4))
    data[['Count', 'time_group']].plot(x='time_group', ax=ax, label='Kafka')

    ax.title.set_text('Kafka')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message scheduled rate (per minute)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_rate_over_time.pdf', bbox_inches='tight')
    plt.close()
    
def __analyse_cassandra(data_path, result_path):

    printinfo(f'Analysing Cassandra data')

    # Check if data path exists
    if not os.path.exists(data_path):
        printerror(f'Data path {data_path} does not exist')
        return

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
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(8, 4))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(8, 4))
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data['Maxiumum'] = data.groupby('time_group')['abs_delay'].transform('max')
    data['95_Percentile'] = data.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    data['Mean'] = data.groupby('time_group')['abs_delay'].transform('mean')
    data['Count'] = data.groupby('time_group')['abs_delay'].transform('count')

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    ax.fill_between(data['time_group'], data['Maxiumum'], data['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax.fill_between(data['time_group'], data['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data['Maxiumum'] > 60))
    ax.title.set_text('Cassandra')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')

    # Message count over time
    fig, ax = plt.subplots(figsize=(8, 4))
    data[['Count', 'time_group']].plot(x='time_group', ax=ax, label='Cassandra')

    ax.title.set_text('Cassandra')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message scheduled rate (per minute)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_rate_over_time.pdf', bbox_inches='tight')
    plt.close()

def __analyse_mysql(data_path, result_path):

    printinfo(f'Analysing MySQL data')

    # Check if data path exists
    if not os.path.exists(data_path):
        printerror(f'Data path {data_path} does not exist')
        return
    
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
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay.pdf', bbox_inches='tight')

    # Message Delay Distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    data['delay'].plot.hist(ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_distribution.pdf', bbox_inches='tight')

    # Message Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(8, 4))
    data['abs_delay'].hist(bins=60, ax=ax)
    ax.set_ylabel("Number of Messages (Frequency)")
    ax.set_xlabel("Message Absolute Delay (Seconds)")
    plt.savefig(f'{result_path}{os.sep}{prefix}messages_abs_delay.pdf', bbox_inches='tight')

    # Message Delay Statistics
    data['delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_delay_statistics.csv')
    data['abs_delay'].describe().to_csv(f'{result_path}{os.sep}{prefix}messages_abs_delay_statistics.csv')

    # Message Delay Over Time
    fig, ax = plt.subplots(figsize=(8, 4))
    data = data.sort_values(by='time')

    data['time_group'] = ((data['time'] - data['time'].min()).astype('timedelta64[s]')/60).astype(int)
    
    data['Maxiumum'] = data.groupby('time_group')['abs_delay'].transform('max')
    data['95_Percentile'] = data.groupby('time_group')['abs_delay'].transform('quantile', 0.95)
    data['Mean'] = data.groupby('time_group')['abs_delay'].transform('mean')
    data['Count'] = data.groupby('time_group')['abs_delay'].transform('count')

    data['time_group'] = data['time_group']/60
    data[['Maxiumum', '95_Percentile', 'Mean', 'time_group']].plot(x='time_group', ax=ax, label='Message Database')
    ax.fill_between(data['time_group'], data['Maxiumum'], data['Mean'], color='#D3F8D3', alpha=0.9, interpolate=True)
    ax.fill_between(data['time_group'], data['Maxiumum'], 61, color='#ff0000', alpha=0.3, interpolate=True, where=(data['Maxiumum'] > 60))
    ax.title.set_text('MySQL')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message Delay (s)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_delay_over_time.pdf', bbox_inches='tight')

    # Message count over time
    fig, ax = plt.subplots(figsize=(8, 4))
    data[['Count', 'time_group']].plot(x='time_group', ax=ax, label='MySQL')

    ax.title.set_text('MySQL')
    ax.set_xlabel('Time (hours)')
    ax.set_ylabel('Message scheduled rate (per minute)')

    plt.savefig(f'{result_path}{os.sep}{prefix}messages_rate_over_time.pdf', bbox_inches='tight')
    plt.close()

def analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result/'
    os.makedirs(result_path, exist_ok=True)

    KAFKA_ENABLED and __analyse_kafka(f'{path}{os.sep}kafka{os.sep}messages.csv', result_path)
    DATABASE_SCHEDULER_CASSANDRA_ENABLED and __analyse_cassandra(f'{path}{os.sep}cassandra{os.sep}messages.csv', result_path)
    DATABASE_SCHEDULER_MYSQL_ENABLED and __analyse_mysql(f'{path}{os.sep}mysql{os.sep}messages.csv', result_path)

    plt.close('all')

