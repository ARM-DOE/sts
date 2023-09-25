#!/bin/bash

# NOTES:
# With postgres running locally:
# - psql postgres
# - create database sts;
# - create role sts_admin with password 'sts';
# - alter role sts_admin with login;
# - alter database sts owner to sts_admin;
# cd control; go test -args -db

basedir=$(dirname $0)

exe=$GOPATH/bin/sts

if [ -z ${STS_HOME+x} ]; then
    export STS_HOME=/var/tmp/sts-autoconf
fi

db_host="localhost"
db_port=5432
db_name="sts"
db_user="sts_admin"
db_pass="sts"
db_table_clients="clients"
db_table_datasets="datasets"

name="stsin-1"
key="12345"
host="localhost"
port="1992"
prfx="/prefix"

serverconfig="{\"name\":\"$name\",\"key\":\"$key\",\"http-host\":\"$host\",\"http-path-prefix\":\"$prfx\",\"http-tls-cert-encoded\":\"\"}"

psql="psql -h $db_host -p $db_port -U $db_user -d $db_name -c"

echo "Cleaning last run..."
$psql "DELETE FROM $db_table_datasets; DELETE FROM $db_table_clients" > /dev/null
rm -rf $STS_HOME

echo "Building ..."

$basedir/../scripts/build.sh \
    --ldflags="-X main.ConfigServer=$serverconfig -X main.StatusInterval=3 -X main.StateInterval=3" \
    && mv $exe ${exe}client
$basedir/../scripts/build.sh \
    && mv $exe ${exe}server

dirs_conf=$(cat <<-END
{
    "cache": ".sts/out",
    "logs": "data/log",
    "out": "data/out",
    "out-follow": true
}
END
)

server_conf=$(cat <<-END
{
    "IN": {
        "control": {
            "host": "${db_host}",
            "port": ${db_port},
            "user": "${db_user}",
            "pass": "${db_pass}",
            "name": "${db_name}",
            "table-clients": "${db_table_clients}",
            "table-datasets": "${db_table_datasets}"
        },
        "dirs": {
            "stage": "data/stage",
            "final": "data/in",
            "logs": "data/log"
        },
        "server": {
            "http-host": "${host}",
            "http-port": ${port},
            "http-path-prefix": "${prfx}"
        }
    }
}
END
)

mkdir -p $STS_HOME/conf
echo $server_conf > $STS_HOME/conf/sts.in.yaml

run_server="${exe}server --debug --mode=in"
run_client="${exe}client --verbose"

echo "Running server ..."

$run_server &

sleep 1

echo "Running client ..."

$run_client &

sleep 1

echo "Making some data ..."

sim=(
    "stsin-1 sm 1000 50 0"
    "stsin-1 xs 500 500 0"
)
for args in "${sim[@]}"; do
    echo "Making data: $args ..."
    $PWD/$basedir/makedata.py $args
done

id=$($psql "SELECT id, upload_key FROM $db_table_clients WHERE upload_key='$key' LIMIT 1" | egrep "\|\s+$key")
id=$(echo $id | cut -d "|" -f1)
id=${id//$'\n'/}
id=${id//$' '/}

echo "Inserting source (client: $id) into database ..."

source_conf=$(cat <<-END
{
    "name": "stsout-1",
    "threads": 4,
    "scan-delay": "10s",
    "cache-age": "5m",
    "min-age": "5s",
    "max-age": "0s",
    "bin-size": "20MB",
    "compress": 0,
    "stat-payload": "true",
    "poll-delay": "2s",
    "poll-interval": "5s",
    "poll-attempts": 1,
    "target": {
        "name": "${name}",
        "key": "${key}:${id}",
        "http-host": "${host}:${port}",
        "http-path-prefix": "${prfx}",
        "http-tls-cert-encoded": ""
    },
    "tags": [
        {
            "pattern": "DEFAULT",
            "delete": "true"
        },
        {
            "pattern": "^sm",
            "method": "none"
        }
    ]
}
END
)

now=$(date +"%Y-%m-%d %T")

add_src=$(cat <<-END
INSERT INTO ${db_table_datasets}
 (name, source_conf, client_id, created_at, updated_at)
 VALUES('stsout-1', '${source_conf}', '${id}', '$now', '$now');
UPDATE ${db_table_clients}
 SET dirs_conf='${dirs_conf}'
    ,verified_at='$now'
    ,updated_at='$now'
 WHERE id='${id}';
END
)
add_src=${add_src//$'\n'/}

$psql "$add_src" > /dev/null

function ctrl_c() {
    echo "Caught signal ..."
    pkill stsserver stsclient
    exit 0
}
trap ctrl_c INT

sleep 5

updated_source_conf=${source_conf/^sm/$'^xs'}

now=$(date +"%Y-%m-%d %T")

update_src=$(cat <<-END
UPDATE ${db_table_datasets}
 SET source_conf='${updated_source_conf}'
 WHERE name='stsout-1' AND client_id='${id}';
UPDATE ${db_table_clients}
 SET dirs_conf='${dirs_conf}'
    ,verified_at='$now'
    ,updated_at='$now'
 WHERE id='${id}';
END
)
update_src=${update_src//$'\n'/}

echo "------------------------------------------------------------------------"
echo "UPDATING CONF ..."
echo "------------------------------------------------------------------------"

$psql "$update_src" > /dev/null

sleep 1

echo "------------------------------------------------------------------------"
echo "Killing server (to test that subsequenet Ctrl-C still stops cleanly) ..."
echo "------------------------------------------------------------------------"
pkill -9 stsserver

while true; do
    sleep 1
done
