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
rm -rf $STS_HOME/data*

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
mkdir -p $STS_HOME/data/out/stsin-2
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/clean.tgz
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

/bin/bash done.sh
