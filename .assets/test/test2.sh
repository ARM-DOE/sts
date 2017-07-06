#!/bin/bash

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe="sts"
bin="$GOPATH/bin/$exe"
debug="--debug"
conf="--conf=run/conf/sts.yaml"
cmd1="$bin $debug $conf --mode=in"
cmd2="$bin $debug $conf --mode=out --loop"
cmd3="$bin $debug --mode=in"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test.yaml $STS_HOME/conf/sts.yaml
cp $basedir/test.in.yaml $STS_HOME/conf/sts.in.yaml

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/clean.tgz

echo "Running sender in background..."
$cmd2 > /dev/null &

sleep 2

echo "Running receiver in background..."
$cmd1 > /dev/null &

sleep 3

echo "Reconfig receiver..."
pkill -f "$cmd1"

sleep 3

mkdir $STS_HOME/data2
mkdir $STS_HOME/data2/log
mv $STS_HOME/data/stage $STS_HOME/data2
mv $STS_HOME/data/in $STS_HOME/data2
mv $STS_HOME/data/log/incoming_from $STS_HOME/data2/log

echo "Restarting..."
$cmd3 > /dev/null &

sleep 30

pkill $exe

echo "-------------------------------------------------------------------------"
echo "ERRORS:"
grep "ERROR" $STS_HOME/data/log/messages/*/*
echo "-------------------------------------------------------------------------"
echo "OUT DUPLICATES:"
cut -d ":" -f 1 $STS_HOME/data*/log/outgoing_to/*/*/* | sort | uniq -c | grep -v " 1 "
echo "-------------------------------------------------------------------------"
echo "IN DUPLICATES (OK):"
cut -d ":" -f 1 $STS_HOME/data*/log/incoming_from/*/*/* | sort | uniq -c | grep -v " 1 "
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data2/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data2/in -type f | sort
