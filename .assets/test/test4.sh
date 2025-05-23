#!/bin/bash

pid_main=$$

basedir=$(dirname $0)
if [ -z ${STS_HOME+x} ]; then
    export STS_HOME=/var/tmp/sts4
fi

bin="$GOPATH/bin/sts"
cmd_server="$bin $args --iport=12345 --debug --mode=in"
cmd_client="$bin $args --iport=54321 --debug --mode=out"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test4.server.yaml $STS_HOME/conf/sts.in.yaml
cp $basedir/test4.client.yaml $STS_HOME/conf/sts.out.yaml
cp $basedir/../localhost.key $STS_HOME
cp $basedir/../localhost.crt $STS_HOME

sim=(
    "stsin-1 xl 100000000     4  600"
    "stsin-1 xs       100  1000  120"
    "stsin-2 lg   1000000   200  150"
    "stsin-2 md    100000   100  300"
    "stsin-2 sm     10000   500  600"
)
cmds=()
for args in "${sim[@]}"; do
    cmds+=("$PWD/$basedir/makedata.py $args")
done
cmds+=("$cmd_server")
cmds+=("$cmd_client --loop")

pids=()
for cmd in "${cmds[@]}"; do
    echo "Starting $cmd ..."
    $cmd > /dev/null &
    pids+=($!)
    sleep 0.5
done

ncmd=${#cmds[@]}
pid_server=${pids[(($ncmd-2))]}
pid_client=${pids[(($ncmd-1))]}

function ctrl_c() {
    echo "Caught signal ..."
    for pid in "${pids[@]}"; do
        echo "Stopping $pid ..."
        kill $pid
    done
    echo "Cleaning out incoming directory ..."
    find $STS_HOME/data/in -type f -exec rm {} \;
    kill $pid_main
    exit 0
}

function clean() {
    while true; do
        sleep 10

        # Remove "in" files older than a minute
        find $STS_HOME/data/in -type f -mmin +1 -exec rm {} \; 2>/dev/null

        # Keep the log files from getting out of hand
        # NOTE: This actually doesn't work as long as STS keeps a file handle
        # to the current log file(s).  Nice try, though.
        # for log in `find $STS_HOME/data/log -type f`; do
        #     n=`wc -l < $log`
        #     if (( "$n" > 20000 )); then
        #         # Remove the first million lines
        #         sed -i -e "1,10000d" $log
        #     fi
        # done
    done
}

function mem() {
    ps=`ps xv | egrep "^\s*$1"`
    echo " $ps" | tr -s ' ' | cut -d ' ' -f 9
}

function check() {
    hosts=( "stsout-1" "stsout-2" )
    types=( "xs" "sm" "md" "lg" "xl" )
    while true; do
        # Log memory usage
        mem $pid_server >> $STS_HOME/mem.server.log
        mem $pid_client >> $STS_HOME/mem.client.log

        sleep 60

        # Check for errors
        errors=`grep ERROR $STS_HOME/data/log/messages/*/*`
        if [ "$errors" ]; then
            cat <(echo "$errors")
            ctrl_c
        fi

        # Check for stuck "out" files
        old=`find $STS_HOME/data/out -type f -mmin +5 2>/dev/null | sort`
        if [ "$old" ]; then
            echo "Found old files in outgoing tree:"
            cat <(echo "$old")
            ctrl_c
        fi

        # Check for duplicates
        dups=`grep "INFO Ignoring duplicate (receive):" $STS_HOME/data/log/messages/*/*`
        if [ "$dups" ]; then
            echo "Detected duplicate!"
            cat <(echo "$dups")
            ctrl_c
        fi

        # Check for out-of-order delivery
        for host in "${hosts[@]}"; do
            for type in "${types[@]}"; do
                recvd=`grep $type. $STS_HOME/data/log/incoming_from/$host/*/*`
                sortd=`cat <(echo "$recvd") | sort`
                match=`diff <(cat <(echo "$recvd")) <(cat <(echo "$sortd"))`
                if [ "$match" ]; then
                    echo "$host:$type: out-of-order:"
                    cat <(echo "$match")
                    ctrl_c
                fi
            done
        done
    done
}

# Handle interrupt signal
trap ctrl_c INT

clean &
pids+=($!)

check &
pids+=($!)

while true; do
    sleep 5
done
