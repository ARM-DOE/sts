#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

pkill sts

exe="$GOPATH/bin/sts"
conf="$PWD/$basedir/../dist.conf.yaml"
debug="--debug"

if [ -z "$1"] || ["z$1" ==  "zboth"]; then
    echo "Cleaning up data..."
    rm -rf $STS_DATA/.sts
    rm -rf $STS_DATA/data
    # cp -rp $STS_DATA/test/.sts $STS_DATA/
    # cp -rp $STS_DATA/test/data $STS_DATA/
    echo "Making new data..."
    $PWD/$basedir/makedata.py
    sleep 1
fi

if [ -z $1 ]; then
    $exe $debug --conf=$conf --mode=both
else
    $exe $debug --conf=$conf --mode=$1
fi

# sleep 8
# pkill sts
# exit
# sleep 5
#
# if [ -n $1 ]; then
#     $exe $debug --conf=$conf 2>&1 &
# else
#     $exe $debug --conf=$conf --mode=$1 2>&1 &
# fi
