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
tar -C $STS_HOME -xzf $PWD/$basedir/crash.tgz
rm -rf $STS_HOME/data/log/messages

ym=`date +%Y%m`
d=`date +%d`
mv $STS_HOME/data/log/incoming_from/stsout-2/201703 \
   $STS_HOME/data/log/incoming_from/stsout-2/$ym

mv $STS_HOME/data/log/incoming_from/stsout-2/$ym/22 \
   $STS_HOME/data/log/incoming_from/stsout-2/$ym/$d

echo "Running..."
$cmd > /dev/null

/bin/bash done.sh
