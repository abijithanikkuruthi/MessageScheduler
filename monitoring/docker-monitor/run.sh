mkdir /logs
cd /logs
rm -rf *

echo "TIMESTAMP                 CONTAINER_ID   NAME                        CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           BLOCK I/O         PIDS" > `date -u +"%Y%m%d.txt"`

while true;
do
    docker stats --no-stream | tail -n +2 | while read line ; do
        echo -e `date`'\t'$line >> `date -u +"%Y%m%d.txt"`
    done
    sleep 10
done