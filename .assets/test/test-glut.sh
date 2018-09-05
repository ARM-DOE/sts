#!/bin/bash

pid_main=$$

basedir=$(dirname $0)
if [ -z ${STS_HOME+x} ]; then
    export STS_HOME=/var/tmp/sts-glut
fi

bin="$GOPATH/bin/sts"
cmd_server1="$bin$VSERVER1 --debug --mode=in"
cmd_client1="$bin$VCLIENT1 --debug --mode=out --loop"

cmd_server2="$bin$VSERVER2 --debug --mode=in"
cmd_client2="$bin$VCLIENT2 --debug --mode=out --loop"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test4.server.yaml $STS_HOME/conf/sts.in.yaml
cp $basedir/test4.client.yaml $STS_HOME/conf/sts.out.yaml

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

echo "Running ..."
$cmd_server1 > /dev/null &
pid_server=$!
$cmd_client1 > /dev/null &
pid_client=$!
pids=( "$pid_client" "$pid_server" )

function monkey() {
    count=0
    while true; do
        sleep 2
        for f in `find $STS_HOME/data/stage -type f -name lg.\*.part`; do
            echo "Monkeying with $f ..."
            dd if=/dev/urandom count=8 bs=8 of=$f conv=notrunc > /dev/null 2>&1
            count=$((count+1))
            if (( count > 2 )); then
                return
            fi
        done
    done
}

monkey &
monkey_pid=$!
pids+=($monkey_pid)

function ctrl_c() {
    echo "Caught signal ..."
    for pid in "${pids[@]}"; do
        echo "Stopping $pid ..."
        kill $pid
    done
    kill $pid_main
    exit 0
}

# Handle interrupt signal
trap ctrl_c INT

# Sleep for a bit to send some of the data
sleep 10

echo "Stopping ..."

# Kill and restart so we can test a glut on start-up
kill $pid_client
kill $pid_server
kill $monkey_pid

# Make more data
for args in "${sim[@]}"; do
    echo "Making data: $args ..."
    $PWD/$basedir/makedata.py $args
done

echo "Restarting server ..."
$cmd_server2 > /dev/null &
pid_server=$!
# Give the server some time to clean up the stage area
sleep 10

echo "Restarting client ..."
$cmd_client2 > /dev/null &
pid_client=$!

echo "Restarting monkey ..."
monkey &
monkey_pid=$!

pids=( "$pid_client" "$pid_server" "$monkey_pid" )

echo "Waiting ..."

# Wait for the client to be done
while true; do
    sleep 5
    out=`find $STS_HOME/data/out -type f 2>/dev/null | sort`
    if [ "$out" ]; then
        lines=($out)
        echo "${#lines[@]} outgoing files left ..."
        continue
    fi
    kill $pid_client
    kill $monkey_pid > /dev/null 2>&1
    break
done

while true; do
    sleep 1
    out=`find $STS_HOME/data/stage -type f 2>/dev/null | sort`
    if [ "$out" ]; then
        lines=($out)
        echo "${#lines[@]} stage files ..."
        continue
    fi
    kill $pid_server
    echo "Done!"
    break
done

echo "Checking for in-order delivery ..."

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

sortd=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort`
uniqd=`uniq -c <(cat <(echo "$sortd")) | grep -v " 1"`
if [ "$uniqd" ]; then
    echo "Incoming Duplicates:"
    cat <(echo "$uniqd")
    echo "FAILED!"
    exit 0
fi

left=`find $STS_HOME/data/out -type f | sort`
if [ "$left" ]; then
    echo "Files Left Behind:"
    cat <(echo "$left")
    echo "FAILED!"
    exit 0
fi

echo "SUCCESS!"
