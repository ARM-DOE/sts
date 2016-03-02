#!/bin/bash

basedir=$(dirname $0)

# if [ -z "$GOPATH" ]
#   then
    export GOPATH=$PWD/$basedir
# fi
if [ -f $GOPATH/bin/sts ]
  then
    echo "-- Cleaning old build"
    rm $GOPATH/bin/sts
    rm -r $GOPATH/pkg
fi

echo "-- Preparing"
mkdir -p $GOPATH/bin

echo "-- Building Dependencies"
go get gopkg.in/yaml.v2
go get util
go get sender
go get receiver

echo "-- Building STS"
go build -o $GOPATH/bin/sts src/sts/sts.go

sts_alive="$(pgrep sts)"
if [[ $sts_alive ]] # do not run tests if either the sender or receiver are currently running
  then
    echo "-- Sender/receiver running, skipping tests"
  else
    if [[ $1 == "-t" ]]
      then
        # rm -r test
        # mkdir -p .test/sender/.sts/bins
        # mkidr -p .test/sender/logs
        # mkdir -p .test/sender/data
        # mkdir -p test/receiver/.sts
        # mkdir -p test/receiver/logs
        # mkdir -p test/receiver/data/stage
        # mkdir -p test/receiver/data/final
        echo "-- Testing"
        go test -cover util
        # go test -cover sender
        # go test -cover receiver
      else
        echo "-- Skipping tests"
    fi
fi
