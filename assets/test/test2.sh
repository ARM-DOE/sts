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
rm -rf $STS_HOME/.sts
rm -rf $STS_HOME/data
rm -rf $STS_HOME/data2

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/sts.tar.gz

echo "Running sender in background..."
$cmd2 > /dev/null &

sleep 2

echo "Running receiver in background..."
$cmd1 > /dev/null &

sleep 3

echo "Reconfig receiver..."
pkill -f "$cmd1"
sleep 10

mkdir $STS_HOME/data2
mkdir $STS_HOME/data2/log
mv $STS_HOME/data/stage $STS_HOME/data2
mv $STS_HOME/data/in $STS_HOME/data2
mv $STS_HOME/data/log/incoming_from $STS_HOME/data2/log

echo "Restarting..."
$cmd3 > /dev/null &

sleep 15

pkill $exe

echo "-------------------------------------------------------------------------"
echo "ERRORS:"
grep "ERROR" $STS_HOME/data/log/messages/*/*
grep "ERROR" $STS_HOME/data2/log/messages/*/*
ototal=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort > /tmp/foo1`
 ouniq=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort | uniq > /tmp/foo2`
itotal=`cut -d ":" -f 1 $STS_HOME/data2/log/incoming_from/*/*/* | sort > /tmp/foo3`
 iuniq=`cut -d ":" -f 1 $STS_HOME/data2/log/incoming_from/*/*/* | sort | uniq > /tmp/foo4`
echo "-------------------------------------------------------------------------"
echo "OUT DUPLICATES:"
diff /tmp/foo1 /tmp/foo2
echo "-------------------------------------------------------------------------"
echo "IN DUPLICATES:"
diff /tmp/foo3 /tmp/foo4
echo "-------------------------------------------------------------------------"
echo "IN vs OUT:"
diff /tmp/foo2 /tmp/foo4
rm /tmp/foo*
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data2/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data2/in -type f | sort
