#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

pkill sts
rm -rf $STS_DATA

# $GOPATH/bin/sts --debug --mode=receive --conf=$PWD/$basedir/../dist.conf.yaml 2>&1 &
#
# sleep 1
#
# $GOPATH/bin/sts --debug --mode=send --conf=$PWD/$basedir/../dist.conf.yaml 2>&1 &
$GOPATH/bin/sts --debug --conf=$PWD/$basedir/../dist.conf.yaml 2>&1 &

$PWD/$basedir/makedata.py
# mkdir -p $PWD/$basedir/run/data/out/stsin-1
# cp ../../main.go $PWD/$basedir/run/data/out/stsin-1

# gdb --pid=$!
