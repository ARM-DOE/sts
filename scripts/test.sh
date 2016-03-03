#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

rm -rf $STS_DATA

$GOPATH/bin/sts --debug --mode=receive --conf=$PWD/$basedir/../conf/dist.receive.yaml 2>&1 &

sleep 1

$GOPATH/bin/sts --debug --mode=send --conf=$PWD/$basedir/../conf/dist.send.yaml 2>&1 &

sleep 1

$PWD/$basedir/makedata.py

# pkill sts
