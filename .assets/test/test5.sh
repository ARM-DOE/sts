#!/bin/bash

# Tests sending the same data from two sources.
# Tests appropriately sending duplicate data.

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe="sts"
bin="$GOPATH/bin/$exe"
debug="--debug"
mode="--mode=auto --loop"
cmd="$bin $debug $mode"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test.yaml $STS_HOME/conf/sts.yaml

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
mkdir -p $STS_HOME/data/out/stsin-2
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/clean.tgz
tar -C $STS_HOME/data/out/stsin-2 -xzf $PWD/$basedir/clean.tgz

echo "Running in background..."
$cmd > /dev/null &

echo "Sleeping..."
sleep 5

echo "Putting the same data in there again..."
tar -C $STS_HOME/data/out/stsin-2 -xzf $PWD/$basedir/clean.tgz
find $STS_HOME/data/out/stsin-2 -type f -exec touch {} \;

echo "Sleeping..."
sleep 20

echo "Done"
pkill $exe

/bin/bash done.sh
