#!/bin/bash

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe_sim="makedata.py"
exe="sts"
bin="$GOPATH/bin/$exe"
args="--debug"
mode="--mode=auto"
cmd_server="$bin $args --mode=in"
cmd_client="$bin $args --mode=out"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test4.yaml $STS_HOME/conf/sts.in.yaml
cp $basedir/test4.yaml $STS_HOME/conf/sts.out.yaml

echo "Running data simulator..."
$PWD/$basedir/$exe_sim &
pid_sim=$!

echo "Running server..."
$cmd_server > /dev/null &
pid_server=$!

echo "Running client..."
$cmd_client --loop > /dev/null &
pid_client=$!

trap ctrl_c INT

function ctrl_c() {
    kill $pid_sim
    kill $pid_client
    kill $pid_server
    bash done.sh
    exit 0
}

while true; do
    sleep 10
    # Remove files older than a minute
    find $STS_HOME/data/in -type f -mmin +1 -exec rm {} \;
done
