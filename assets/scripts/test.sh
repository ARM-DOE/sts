#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

pkill sts
rm -rf $STS_DATA

exe="$GOPATH/bin/sts"
conf="$PWD/$basedir/../dist.conf.yaml"
debug="--debug"

if [[ ! -n $1 ]] || [[ $1 ==  "send" ]]; then
    $PWD/$basedir/makedata.py
fi

if [ -n $1 ]; then
    $exe $debug --conf=$conf 2>&1 &
else
    $exe $debug --conf=$conf --mode=$1 2>&1 &
fi
