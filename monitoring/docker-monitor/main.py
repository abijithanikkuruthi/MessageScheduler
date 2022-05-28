from common import Config, getTime, printdebug, printsuccess, printerror, printheader, save_url_to_file, excpetion_info
from constants import CONFIG_FILE, DOCKER_MONITOR_LOG_FILE, JOB_LOG_FILE, LOGS_FOLDER, SCHEDULER_SERVER_URL, REQUEST_ERROR_WAIT_TIME, MONITOR_INTERVAL
import os
import time
import docker

WORKING_FOLDER = f'{LOGS_FOLDER}{os.sep}{getTime("%Y-%m-%d %H-%M-%S")}'

CONFIG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{CONFIG_FILE}'

DOCKER_MONITOR_LOG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{DOCKER_MONITOR_LOG_FILE}'

JOB_LOG_FILE_PATH = f'{WORKING_FOLDER}{os.sep}{JOB_LOG_FILE}'
JOB_LOG_URL = f'{SCHEDULER_SERVER_URL}/api/job_log'

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
            processed_stats[stat] = 'error'
            printerror(f'Error processing stats: {stat} {e}')
    return dict(sorted(processed_stats.items(), reverse=True))

def save_stats(stats):
    log_str = ', '.join([str(v) for k, v in stats.items()])
                
    if not os.path.exists(DOCKER_MONITOR_LOG_FILE_PATH):
        with open(DOCKER_MONITOR_LOG_FILE_PATH, 'a+') as f:
            header_str = ', '.join(stats.keys())
            f.write(f'{header_str}\n')
            f.write(f'{log_str}\n')
    else:
        with open(DOCKER_MONITOR_LOG_FILE_PATH, 'a') as f:
            f.write(f'{log_str}\n')
    
if __name__ == '__main__':
    
    printheader(f'Starting docker-monitor in folder: {WORKING_FOLDER}')

    os.makedirs(WORKING_FOLDER, exist_ok=True)
    
    # Saving Job Configuration
    save_url_to_file(Config.CONFIG_URL, CONFIG_FILE_PATH)

    while True:
        try:
            # Saving Docker Monitor Log
            client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
            for container in client.containers.list():
                if container.name not in ['renderer', 'docker-monitor']:
                    stats = container.stats(stream=False, decode=None)
                    processed_stats = process_stats(stats)
                    save_stats(processed_stats)

            # Saving Job Log
            save_url_to_file(JOB_LOG_URL, JOB_LOG_FILE_PATH)

        except Exception as e:
            printerror(f'main(): {e}')
            excpetion_info()
            time.sleep(REQUEST_ERROR_WAIT_TIME)
            continue

        time.sleep(MONITOR_INTERVAL)