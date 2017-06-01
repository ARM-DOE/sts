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
tar -C $STS_HOME -xzf $PWD/$basedir/crash.tgz
rm -rf $STS_HOME/data/log/messages

echo "Running..."
$cmd > /dev/null

/bin/bash done.sh
