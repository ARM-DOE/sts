#!/bin/bash

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe="sts"
bin="$GOPATH/bin/$exe"
debug="--debug"
mode="--mode=both"
cmd="$bin $debug $mode"

echo "Cleaning last run..."
rm -rf $STS_HOME/.sts
rm -rf $STS_HOME/data

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
tar -C $STS_HOME/data/out/stsin-1 -xf $PWD/$basedir/sts.tar

# cp -rp $STS_HOME/test/.sts $STS_HOME/
# cp -rp $STS_HOME/test/data $STS_HOME/
# echo "Making new data..."
# $PWD/$basedir/makedata.py
# sleep 1

echo "Running in background..."
$cmd > /dev/null &

sleep 4 # Wait for it do some stuff.

echo "Simulating crash..."
pkill $exe
sleep 1 # Wait for it to die.

echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort

echo "Restarting..."
$cmd > /dev/null

# TODO: automate log/dir checking to confirm:
# 1) files sent in order
# 2) no file left behind
# 3) no duplicates

echo "-------------------------------------------------------------------------"
echo "ERRORS:"
grep "ERROR" $STS_HOME/data/log/messages/*/*
echo "-------------------------------------------------------------------------"
echo "OUT DUPLICATES:"
total=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort > /tmp/foo1`
 uniq=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort | uniq > /tmp/foo2`
diff /tmp/foo1 /tmp/foo2
rm /tmp/foo*
echo "-------------------------------------------------------------------------"
echo "OUT DIR:"
find $STS_HOME/data/out -type f
echo "-------------------------------------------------------------------------"
echo "STAGE DIR:"
find $STS_HOME/data/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort