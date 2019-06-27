#!/bin/bash

race=""
ldflags=""
outname="sts"

for i in "$@"; do
    case $i in
        -r|--race)
        race="-race"
        shift
        ;;
        --ldflags=*)
        ldflags="${i#*=}"
        shift
        ;;
        --os=*)
        GOOS="${i#*=}"
        shift
        ;;
        --outname=*)
        outname="${i#*=}"
        shift
        ;;
    esac
done

if [[ "$GOOS" == "windows" ]]; then
    outname="$outname.exe"
fi

basedir=$(dirname $0)

if [ -f $GOPATH/bin/sts ]; then
    echo "-- Cleaning Old Build"
    rm $GOPATH/bin/sts
    rm -rf $GOPATH/pkg
fi

echo "-- Preparing"
mkdir -p $GOPATH/bin

echo "-- Building Dependencies"
go get gopkg.in/yaml.v2
go get github.com/lib/pq
go get github.com/jmoiron/sqlx
go get github.com/alecthomas/units
go get github.com/stackimpact/stackimpact-go

date=`date -u '+%Y-%m-%d %H:%M:%S'`
vers=$APR_VERSION
if [ -z "$vers" ]; then
    vers=`git describe --tags 2>/dev/null`
    if [ -z "$vers" ]; then
        vers="v0.0.0"
    fi
fi

echo "-- Building Executable"
go build -o $GOPATH/bin/$outname $race \
    -ldflags="-X 'main.BuildTime=$date UTC' -X 'main.Version=$vers' $ldflags" \
    $GOPATH/src/code.arm.gov/dataflow/sts/main/*.go
