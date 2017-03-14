#!/bin/bash

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe="sts"
bin="$GOPATH/bin/$exe"
debug="--debug"
mode="--mode=auto"
cmd="$bin $debug $mode"

echo "Cleaning last run..."
rm -rf $STS_HOME/.sts
rm -rf $STS_HOME/data

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
mkdir -p $STS_HOME/data/out/stsin-2
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/sts.tar.gz
mv $STS_HOME/data/out/stsin-1/collection $STS_HOME/data/out/stsin-2

# cp -rp $STS_HOME/test/.sts $STS_HOME/
# cp -rp $STS_HOME/test/data $STS_HOME/
# echo "Making new data..."
# $PWD/$basedir/makedata.py
# sleep 1

echo "Running in background..."
$cmd > /dev/null &

sleep 1 # Wait for it do some stuff.

echo "Simulating crash..."
pkill $exe
sleep 3 # Wait for it to die.

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
ototal=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort > /tmp/foo1`
 ouniq=`cut -d ":" -f 1 $STS_HOME/data/log/outgoing_to/*/*/* | sort | uniq > /tmp/foo2`
itotal=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort > /tmp/foo3`
 iuniq=`cut -d ":" -f 1 $STS_HOME/data/log/incoming_from/*/*/* | sort | uniq > /tmp/foo4`
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
find $STS_HOME/data/stage -type f
echo "-------------------------------------------------------------------------"
echo "IN DIR:"
find $STS_HOME/data/in -type f | sort
