# basic file commands executed in container

# view files in directory
docker exec bor-db ls -l /var/lib/mysql-files/ftpetl/
docker exec bor-db ls -l /var/lib/mysql-files/ftpetl/outgoing

# view file 
docker exec bor-db head -10 /var/lib/mysql-files/ftpetl/outgoing/holdings_yyyymmdd.txt
docker exec bor-db head -10 /var/lib/mysql-files/ftpetl/incoming/holdweb-20241231.csv

# copy local file to container volume
docker cp tests/data/holdweb-20241231.csv bor-db:/var/lib/mysql-files/ftpetl/incoming/holdweb-20241231.csv

# delete specific file in container volume
docker exec bor-db rm /var/lib/mysql-files/ftpetl/outgoing/holdings_20250414.txt

docker exec bor-db rm /var/lib/mysql-files/ftpetl/incoming/holdweb-20241231.csv

