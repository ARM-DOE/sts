#!/bin/bash

race=""
ldflags=""
outdir=$GOPATH/bin
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
        export GOOS="${i#*=}"
        shift
        ;;
        --outdir=*)
        outdir="${i#*=}"
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
rootdir="$basedir/../../"

cd $rootdir

if [ -f $outdir/$outname ]; then
    echo "-- Cleaning Old Build"
    rm $outdir/$outname
fi

echo "-- Preparing"
mkdir -p $outdir

echo "-- Building Dependencies"
go mod download

date=`date -u '+%Y-%m-%d %H:%M:%S'`
vers=$APR_VERSION
if [ -z "$vers" ]; then
    vers=`git describe --tags 2>/dev/null`
    if [ -z "$vers" ]; then
        vers="v0.0.0"
    fi
fi

echo "-- Building Executable (GOOS='$GOOS', GOARCH='$GOARCH')"
go build -o $outdir/$outname $race \
    -ldflags="-X 'main.BuildTime=$date UTC' -X 'main.Version=$vers' $ldflags" \
    ./main/*.go
