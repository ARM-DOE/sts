#!/bin/bash

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
cert=$(base64 $basedir/../localhost.crt)

serverconfig="{\"name\":\"$name\",\"key\":\"$key\",\"http-host\":\"$host\",\"http-tls-cert-encoded\":\"$cert\"}"

psql="psql -h $db_host -p $db_port -U $db_user -d $db_name -c"

echo "Cleaning last run..."
$psql "DELETE FROM $db_table_datasets; DELETE FROM $db_table_clients" > /dev/null
rm -rf $STS_HOME

echo "Building ..."

$basedir/../scripts/build.sh \
    --ldflags="-X main.ConfigServer=$serverconfig -X main.StatusInterval=3" \
    && mv $exe ${exe}client
$basedir/../scripts/build.sh

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
            "http-tls-cert": "${PWD}/${basedir}/../localhost.crt",
            "http-tls-key": "${PWD}/${basedir}/../localhost.key"
        }
    }
}
END
)

mkdir -p $STS_HOME/conf
echo $server_conf > $STS_HOME/conf/sts.in.yaml

run_server="$exe --debug --mode=in"
run_client="${exe}client"

echo "Running server ..."

$run_server &

sleep 1

echo "Running client ..."

$run_client &

sleep 1

echo "Making some data ..."

sim=(
    "stsin-1 xs 1000 50 0"
)
for args in "${sim[@]}"; do
    echo "Making data: $args ..."
    $PWD/$basedir/makedata.py $args
done

echo "Inserting source into database ..."

id=$($psql "SELECT id FROM $db_table_clients LIMIT 1" | egrep "^\s*$key:")
id=${id//$'\n'/}

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
        "key": "${id}",
        "http-host": "${host}:${port}",
        "http-tls-cert-encoded": "${cert}"
    },
    "tags": [
        {
            "pattern": "DEFAULT",
            "delete": "true"
        }
    ]
}
END
)

now=$(date +"%Y-%m-%d %T")

add_src=$(cat <<-END
INSERT INTO ${db_table_datasets}
 (name, source_conf, client_id, created_at, updated_at)
 VALUES('stsout-1', '${source_conf}', trim('${id}'), '$now', '$now');
UPDATE ${db_table_clients}
 SET dirs_conf='${dirs_conf}'
    ,verified_at='$now'
    ,updated_at='$now'
 WHERE id=trim('${id}');
END
)
add_src=${add_src//$'\n'/}

$psql "$add_src" > /dev/null

function ctrl_c() {
    echo "Caught signal ..."
    pkill sts stsclient
    exit 0
}
trap ctrl_c INT

while true; do
    sleep 1
done
