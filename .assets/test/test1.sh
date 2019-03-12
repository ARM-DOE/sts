#!/bin/bash

basedir=$(dirname $0)
export STS_HOME=$PWD/$basedir/run

exe="sts"
bin="$GOPATH/bin/$exe"
debug="--debug"
mode="--mode=auto"
cmd="$bin $debug $mode"

echo "Cleaning last run..."
rm -rf $STS_HOME
mkdir -p $STS_HOME/conf
cp $basedir/test.yaml $STS_HOME/conf/sts.yaml

echo "Staging test data..."
mkdir -p $STS_HOME/data/out/stsin-1
mkdir -p $STS_HOME/data/out/stsin-2
tar -C $STS_HOME/data/out/stsin-1 -xzf $PWD/$basedir/clean.tgz

mkdir -p $STS_HOME/data/out/stsin-1/newdir
touch $STS_HOME/data/out/stsin-1/newdir/betterignore
echo "some content" > $STS_HOME/data/out/stsin-1/newdir/.betterignore
echo "some content" > $STS_HOME/data/out/stsin-1/newdir/bettersend

# For testing a symbolically linked file:
# mv $STS_HOME/data/out/stsin-1/info/nsastsC1.20130806.134501.asc $STS_HOME/data/out
# mkdir -p $STS_HOME/data/out/stsin-2/info
# ln -s $STS_HOME/data/out/nsastsC1.20130806.134501.asc $STS_HOME/data/out/stsin-2/info/nsastsC1.20130806.134501.asc

# For testing a symbolically linked directory (requires out-follow: true in conf):
# mv $STS_HOME/data/out/stsin-1/collection $STS_HOME/data/out/stsin-3
# ln -s $STS_HOME/data/out/stsin-3 $STS_HOME/data/out/stsin-2

# cp -rp $STS_HOME/test/.sts $STS_HOME/
# cp -rp $STS_HOME/test/data $STS_HOME/
# echo "Making new data..."
# $PWD/$basedir/makedata.py
# sleep 1

echo "Running in background..."
$cmd > /dev/null &

sleep 8 # Wait for it do some stuff.

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
