#!/bin/bash

basedir=$(dirname $0)

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
go get github.com/ARM-DOE/sts/util
go get github.com/ARM-DOE/sts/sender
go get github.com/ARM-DOE/sts/receiver

echo "-- Building STS"
go build -o $GOPATH/bin/sts $GOPATH/src/github.com/ARM-DOE/sts/main.go

sts_alive="$(pgrep sts)"
if [[ $sts_alive ]] # do not run tests if either the sender or receiver are currently running
  then
    echo "-- Sender/receiver running, skipping tests"
  else
    if [[ $1 == "-t" ]]
      then
        echo "-- Testing"
        go test -cover util
        # go test -cover sender
        # go test -cover receiver
      else
        echo "-- Skipping tests"
    fi
fi
