#!/bin/bash

pid_main=$$

basedir=$(dirname $0)
if [ -z ${STS_TEST+x} ]; then
    export STS_HOME=/var/tmp/sts
fi

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test4.server.yaml $STS_HOME/conf/sts.in.yaml
cp $basedir/test4.client.yaml $STS_HOME/conf/sts.out.yaml

bin="$GOPATH/bin/sts"

clientdebug="--debug "
serverdebug="--debug "

clientoutput="/dev/null"
serveroutput="/dev/null"

sleepfactor=1
if [ ! -z "$clientdebug" ]; then
    clientoutput="$STS_HOME/data/log/debug.client"
    mkdir -p $(dirname $clientoutput)
fi
if [ ! -z "$serverdebug" ]; then
    serveroutput="$STS_HOME/data/log/debug.server"
    mkdir -p $(dirname $serveroutput)
fi

cmd_client1="$bin$VCLIENT1 $clientdebug--mode=out --loop"
cmd_client2="$bin$VCLIENT2 $clientdebug--mode=out --loop"

cmd_server1="$bin$VSERVER1 $serverdebug--mode=in"
cmd_server2="$bin$VSERVER2 $serverdebug--mode=in"

sim=(
    "stsin-1 xs       100   5000 0"
    "stsin-1 xl 100000000      2 0"
    "stsin-2 lg  10000000     20 0"
    "stsin-2 md    100000    500 0"
    "stsin-2 sm     10000   1000 0"
)
for args in "${sim[@]}"; do
    echo "Making data: $args ..."
    $PWD/$basedir/makedata.py $args
done

echo "Running server ..."
$cmd_server1 > $serveroutput &
pid_server=$!

echo "Running client ..."
$cmd_client1 > $clientoutput &
pid_client=$!
pids=( "$pid_client" "$pid_server" )

function monkey() {
    count_stg=0
    count_out=0
    while (( count_stg < 3 || count_out < 3 )); do
        sleep $(( 5 * $sleepfactor ))
        if (( count_stg < 3 )); then
            for f in `find $STS_HOME/data/stage -type f -name lg.\*.part`; do
                echo "Monkeying with $f ..."
                dd if=/dev/urandom count=8 bs=8 of=$f conv=notrunc > /dev/null 2>&1
                count_stg=$((count_stg+1))
            done
        fi
        if (( count_out < 3 )); then
            for f in `find $STS_HOME/data/out -type f -name lg.\* | head -1`; do
                echo "Monkeying with $f ..."
                dd if=/dev/urandom count=8 bs=8 of=$f conf=notrunc > /dev/null 2>&1
                count_out=$((count_out+1))
            done
        fi
    done
}

monkey &
monkey_pid=$!
pids+=($monkey_pid)

function ctrl_c() {
    echo "Caught signal ..."
    for pid in "${pids[@]}"; do
        echo "Stopping $pid ..."
        kill -9 $pid > /dev/null 2>&1
    done
    kill $pid_main
    exit 0
}

# Handle interrupt signal
trap ctrl_c INT

# Sleep for a bit to send some of the data
sleep $(( 20 * $sleepfactor ))

echo "Stopping ..."

# Kill and restart so we can test a glut on start-up
kill -9 $pid_client
kill -9 $pid_server
kill $monkey_pid > /dev/null 2>&1

# Make more data
for args in "${sim[@]}"; do
    echo "Making data: $args ..."
    $PWD/$basedir/makedata.py $args
done

echo "Restarting server ..."
$cmd_server2 >> $serveroutput &
pid_server=$!
# Give the server some time to clean up the stage area
sleep $(( 10 * $sleepfactor ))

echo "Restarting client ..."
$cmd_client2 >> $clientoutput &
pid_client=$!

echo "Restarting monkey ..."
monkey &
monkey_pid=$!

pids=( "$pid_client" "$pid_server" "$monkey_pid" )

echo "Waiting ..."

# Wait for the client to be done
while true; do
    sleep $(( 5 * $sleepfactor ))
    out=`find $STS_HOME/data/out -type f 2>/dev/null | sort`
    if [ "$out" ]; then
        lines=($out)
        echo "${#lines[@]} outgoing files left ..."
        continue
    fi
    kill -9 $pid_client
    kill $monkey_pid > /dev/null 2>&1
    break
done

# Trigger a stage cleanup manually
curl 'localhost:1992/clean?block&minage=1'

while true; do
    sleep $(( 1 * $sleepfactor ))
    out=`find $STS_HOME/data/stage -type f 2>/dev/null | sort | egrep -v '.part$'`
    if [ "$out" ]; then
        lines=($out)
        echo "${#lines[@]} stage files ..."
        continue
    fi
    kill $pid_server
    echo "Done!"
    break
done

echo "Checking results ..."

hosts=( "stsout-1" "stsout-2" )
types=( "xs" "sm" "md" "lg" "xl" )

for host in "${hosts[@]}"; do
    for type in "${types[@]}"; do
        recvd=`grep $type. $STS_HOME/data/log/incoming_from/$host/*/*`
        sortd=`cat <(echo "$recvd") | sort`
        match=`diff <(cat <(echo "$recvd")) <(cat <(echo "$sortd"))`
        if [ "$match" ]; then
            echo "$host:$type: Out-of-Order:"
            cat <(echo "$match")
            echo "FAILED!"
            exit 0
        fi
    done
done

left=`find $STS_HOME/data/out -type f | sort`
if [ "$left" ]; then
    echo "Files Left Behind:"
    cat <(echo "$left")
    echo "FAILED!"
    exit 0
fi

stale=`find $STS_HOME/data/stage -type f | sort`
if [ "$stale" ]; then
    echo "Stray Files Found:"
    cat <(echo "$stale")
    echo "FAILED!"
    exit 0
fi

sortd=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort`
uniqd=`uniq -c <(cat <(echo "$sortd")) | grep -v " 1"`
if [ "$uniqd" ]; then
    echo "Incoming Duplicates (probably OK):"
    cat <(echo "$uniqd")
    echo "SUCCESS?"
    exit 0
fi

echo "SUCCESS!"
