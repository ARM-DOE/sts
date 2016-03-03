#!/bin/bash

basedir=$(dirname $0)
export STS_DATA=$PWD/$basedir/run

rm -r $STS_DATA

$basedir/bin/sts --debug --mode=receive --conf=$basedir/conf/dist.receive.yaml 2>&1 &

sleep 1

$basedir/bin/sts --debug --mode=send --conf=$basedir/conf/dist.send.yaml 2>&1 &

sleep 1

# cp $basedir/test/test_files/*.* $basedir/run/data/out

$PWD/$basedir/scripts/makedata.py

# pkill sts
