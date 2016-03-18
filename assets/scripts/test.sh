#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

pkill sts

exe="$GOPATH/bin/sts"
conf="$PWD/$basedir/../dist.conf.yaml"
debug="--debug"

if [[ ! -n $1 ]] || [[ $1 ==  "send" ]]; then
    rm -rf $STS_DATA/.sts
    rm -rf $STS_DATA/data
    # cp -rp $STS_DATA/test/.sts $STS_DATA/
    # cp -rp $STS_DATA/test/data $STS_DATA/
    $PWD/$basedir/makedata.py
fi

if [ -n $1 ]; then
    $exe $debug --conf=$conf 2>&1 &
else
    $exe $debug --conf=$conf --mode=$1 2>&1 &
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
