import os
import pandas as pd
import matplotlib.pyplot as plt

def analyse(config):
    path = config['data_path']

    result_path = f'{path}{os.sep}result/'
    os.makedirs(result_path, exist_ok=True)

    message_database_data_path = f'{path}{os.sep}message-database{os.sep}messages.csv'
    database_scheduler_data_path = f'{path}{os.sep}database-scheduler{os.sep}messages.csv'

    md_df = pd.read_csv(message_database_data_path)
    ds_df = pd.read_csv(database_scheduler_data_path)

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

    # Delay Computations
    md_df.time = pd.to_datetime(md_df.time)
    md_df.__sm_worker_timestamp.fillna(md_df.__sm_mh_timestamp, inplace=True)
    md_df.__sm_worker_timestamp = pd.to_datetime(md_df.__sm_worker_timestamp)
    md_df['delay'] = (md_df.time - md_df.__sm_worker_timestamp).astype('timedelta64[s]')
    md_df['abs_delay'] = abs((md_df.time - md_df.__sm_worker_timestamp).astype('timedelta64[s]'))

    ds_df.time = pd.to_datetime(ds_df.time)
    ds_df.sm_recieved_time = pd.to_datetime(ds_df.sm_recieved_time)
    ds_df['delay'] = (ds_df.time - ds_df.sm_recieved_time).astype('timedelta64[s]')
    ds_df['abs_delay'] = abs((ds_df.time - ds_df.sm_recieved_time).astype('timedelta64[s]'))

    # Delay Histogram
    fig, ax = plt.subplots(figsize=(25, 9))
    hist_df = pd.DataFrame()
    hist_df['Kafka'] = md_df.delay
    hist_df['Cassandra'] = ds_df.delay
    axarr = hist_df.hist(bins=60, ax=ax, sharey=True, sharex=True, legend=False, ylabelsize=12, xlabelsize=12, figsize=(25, 9))
    for ax in axarr.flatten():
        ax.set_xlabel("Message Delivery Delay (s)")
        ax.set_ylabel("Message Count")
    fig.savefig(f'{result_path}/message_delay_comparison.pdf', bbox_inches='tight')

    # Absolute Delay Histogram
    fig, ax = plt.subplots(figsize=(25, 9))
    hist_df = pd.DataFrame()
    hist_df['Kafka'] = md_df.abs_delay
    hist_df['Cassandra'] = ds_df.abs_delay
    axarr = hist_df.hist(bins=60, ax=ax, sharey=True, sharex=True, legend=False, ylabelsize=12, xlabelsize=12, figsize=(25, 9))
    for ax in axarr.flatten():
        ax.set_xlabel("Absolute difference between scheduled time and delivered time (s)")
        ax.set_ylabel("Message Count")
    fig.savefig(f'{result_path}/message_abs_delay_comparison.pdf', bbox_inches='tight')

    # Delay Statistics
    md_df.delay.describe().to_csv(f'{result_path}/kafka_messages_delay.csv')
    md_df.abs_delay.describe().to_csv(f'{result_path}/kafka_messages_abs_delay.csv')
    ds_df.delay.describe().to_csv(f'{result_path}/cassandra_messages_delay.csv')
    ds_df.abs_delay.describe().to_csv(f'{result_path}/cassandra_messages_abs_delay.csv')

    plt.close('all')