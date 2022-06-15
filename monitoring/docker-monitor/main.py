from common import getTime, printdebug, printsuccess, printerror, printheader, save_url_to_file, excpetion_info
from constants import *
import os
import time
import docker

WORKING_FOLDER = f'{LOGS_FOLDER}'

DOCKER_MONITOR_LOG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{DOCKER_MONITOR_LOG_FILE}'

KAFKA_SCHEDULER_CONFIG_URL = f'{KAFKA_SCHEDULER_SERVER_URL}/config'
KAFKA_SCHEDULER_CONFIG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{KAFKA_SCHEDULER_CONFIG_FILE}'
KAFKA_SCHEDULER_JOB_LOG_URL = f'{KAFKA_SCHEDULER_SERVER_URL}/api/job_log'
KAFKA_SCHEDULER_JOB_LOG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{KAFKA_SCHEDULER_JOB_LOG_FILE}'

def process_stats(stats):
    processed_stats = {}

    # ['read', 'preread', 'pids_stats', 'blkio_stats', 'num_procs', 'storage_stats', 'cpu_stats', 'precpu_stats', 'memory_stats', 'name', 'id', 'networks']
    for stat in stats.keys():
        try:
            if stat=='read':
                processed_stats['time'] = getTime()
            elif stat=='pids_stats':
                processed_stats['pids'] = stats[stat]['current']
            elif stat=='blkio_stats':
                processed_stats['blkio_read (MB)'] = stats[stat]['io_service_bytes_recursive'][0]['value']/(1024*1024)
                processed_stats['blkio_write (MB)'] = stats[stat]['io_service_bytes_recursive'][1]['value']/(1024*1024)
            elif stat=='cpu_stats':
                processed_stats['cpu_system (s)'] = stats[stat]['cpu_usage']['total_usage'] - stats[stat]['cpu_usage']['usage_in_kernelmode']
                processed_stats['cpu_user (s)'] = stats[stat]['cpu_usage']['total_usage'] - processed_stats['cpu_system (s)']
            elif stat=='memory_stats':
                processed_stats['memory_usage (MB)'] = stats[stat]['usage']/(1024*1024)
            elif stat=='networks':
                processed_stats['net_rx (MB)'] = stats[stat]['eth0']['rx_bytes']/(1024*1024)
                processed_stats['net_tx (MB)'] = stats[stat]['eth0']['tx_bytes']/(1024*1024)
            elif stat=='name':
                processed_stats['name'] = stats[stat].replace("/","")
        except Exception as e:
            processed_stats[stat] = 0
            printerror(f'Error processing stats: {stat} {e}')
            return False
    return dict(sorted(processed_stats.items(), reverse=True))

def save_stats(stats):
    log_str = ', '.join([str(v) for k, v in stats.items()])
                
    if not os.path.exists(DOCKER_MONITOR_LOG_FILE_PATH):
        os.makedirs(WORKING_FOLDER, exist_ok=True)
        with open(DOCKER_MONITOR_LOG_FILE_PATH, 'a+') as f:
            header_str = ', '.join(stats.keys())
            f.write(f'{header_str}\n')
            f.write(f'{log_str}\n')
    else:
        with open(DOCKER_MONITOR_LOG_FILE_PATH, 'a') as f:
            f.write(f'{log_str}\n')
    
if __name__ == '__main__':
    
    printsuccess(f'Starting docker monitor service')

    os.makedirs(WORKING_FOLDER, exist_ok=True)
    
    # Saving Kafka Job Configuration
    KAFKA_ENABLED and save_url_to_file(KAFKA_SCHEDULER_CONFIG_URL, KAFKA_SCHEDULER_CONFIG_FILE_PATH)
    client = docker.DockerClient(base_url='unix:///var/run/docker.sock')

    while True:
        start_time = time.time()
        try:
            # Saving Docker Monitor Log for each container
            for container in client.containers.list():
                if container.name not in ['renderer', 'docker-monitor']:
                    stats = container.stats(stream=False, decode=None)
                    processed_stats = process_stats(stats)
                    processed_stats and save_stats(processed_stats)

            # Saving Kafka Scheduler Job Log
            KAFKA_ENABLED and save_url_to_file(KAFKA_SCHEDULER_JOB_LOG_URL, KAFKA_SCHEDULER_JOB_LOG_FILE_PATH)

        except Exception as e:
            printerror(f'main(): {e}')
            time.sleep(1)
            continue
        
        end_time = time.time()
        time.sleep(max(0, DOCKER_MONITOR_INTERVAL - (end_time - start_time)))