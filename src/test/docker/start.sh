mkdir /tmp/layers-store-test
cd /tmp/layers-store-test

# use clean docker containers
docker kill layers-store-test-geoserver
docker kill layers-store-test-postgis
docker rm layers-store-test-postgis
docker rm layers-store-test-geoserver
docker run --name "layers-store-test-postgis" -p 5433:5432 -d -t kartoza/postgis:9.6-2.4
docker run --name "layers-store-test-geoserver"  --link layers-store-test-postgis:layers-store-test-postgis -p 8078:8080 -d -t kartoza/geoserver

wget -O layersdb.sql https://github.com/AtlasOfLivingAustralia/spatial-database/raw/master/layersdb.sql

sed 's/layersdb/gis/g' < layersdb.sql  > schema.sql |
sed 's/drop database gis//g' > schema.sql

docker cp schema.sql layers-store-test-postgis:/tmp/

# wait for postgis
while ! nc -z localhost 5433; do
  sleep 0.1
done

# wait for geoserver
while ! nc -z localhost 8078; do
  sleep 0.1
done

wget -O geoserver.sh https://github.com/AtlasOfLivingAustralia/ala-install/raw/master/ansible/roles/geoserver/templates/geoserver.sh
wget -O geoserver.points.xml https://github.com/AtlasOfLivingAustralia/ala-install/raw/master/ansible/roles/geoserver/templates/geoserver.points.xml
wget -O geoserver.objects.xml https://github.com/AtlasOfLivingAustralia/ala-install/raw/master/ansible/roles/geoserver/templates/geoserver.objects.xml
wget -O geoserver.distributions.xml https://github.com/AtlasOfLivingAustralia/ala-install/raw/master/ansible/roles/geoserver/templates/geoserver.distributions.xml

docker exec -e PGPASSWORD=docker layers-store-test-postgis psql -h localhost -U docker gis -f /tmp/schema.sql

# add some data into some tables for the geoserver script to work
docker exec -e PGPASSWORD=docker layers-store-test-postgis psql -h localhost -U docker gis -c "insert into objects (pid, id) values ('-1', '')"
docker exec -e PGPASSWORD=docker layers-store-test-postgis psql -h localhost -U docker gis -c "insert into distributions (spcode) values (-1)"

sed 's/{{tomcat_webapps}}/\/usr\/local\/tomcat\/webapps/g' < geoserver.sh |
sed 's/{{geoserver_url}}/http:\/\/localhost:8078\/geoserver/g' |
sed 's/{{geoserver_username}}/admin/g' |
sed 's/{{geoserver_password}}/geoserver/g' |
sed "s/{{layers_db_host \| default('localhost')}}/layers-store-test-postgis/g" |
sed "s/{{layers_db_port \| default('5432')}}/5432/g" |
sed "s/{{layers_db_name \| default('layersdb')}}/gis/g" |
sed "s/{{layers_db_username \| default('postgres')}}/docker/g" |
sed 's/{{layers_db_password}}/docker/g' > init.sh

chmod a+x init.sh
./init.sh

