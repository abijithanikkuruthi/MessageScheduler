mkdir -p /logs
cd /logs

foldername=`date`
mkdir "$foldername"
cd "$foldername"

echo "TIMESTAMP                       CONTAINER_ID   NAME                        CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           BLOCK I/O         PIDS" > docker_monitor.log

while true;
do
    # DOCKER STATS
    docker stats --no-stream | tail -n +2 | while read line ; do
        echo -e `date`'\t'$line >> docker_monitor.log
    done

    # JOB LOG API
    wget --timeout=5 --tries=1 -q -O job_log.log $JOB_LOG_API_URL #> /dev/null 2>&1
    sleep 10
done