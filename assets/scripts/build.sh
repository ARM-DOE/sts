#!/bin/bash

race=""
if [[ $1 == "-race" ]]
  then
    race="-race"
fi
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
go get github.com/alecthomas/units
go get github.com/davecgh/go-spew/spew
go get $race github.com/ARM-DOE/sts/fileutils
go get $race github.com/ARM-DOE/sts/httputils
go get $race github.com/ARM-DOE/sts/logging
go get $race github.com/ARM-DOE/sts/pathutils

echo "-- Building STS"
go build -o $GOPATH/bin/sts $race $GOPATH/src/github.com/ARM-DOE/sts/*.go
