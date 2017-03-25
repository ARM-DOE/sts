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
    rm -rf $GOPATH/pkg
fi

echo "-- Preparing"
mkdir -p $GOPATH/bin

echo "-- Building Dependencies"
go install gopkg.in/yaml.v2
go install github.com/alecthomas/units
go install github.com/davecgh/go-spew/spew
go get $race github.com/ARM-DOE/sts/fileutils
go get $race github.com/ARM-DOE/sts/httputils
go get $race github.com/ARM-DOE/sts/logging

date=`date -u '+%Y-%m-%d %H:%M:%S'`
vers=`git describe --tags`

echo "-- Building STS"
go build -o $GOPATH/bin/sts $race \
    -ldflags="-X 'main.BuildTime=$date UTC' -X 'main.Version=$vers'" \
    $GOPATH/src/github.com/ARM-DOE/sts/*.go
