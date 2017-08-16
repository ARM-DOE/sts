#!/bin/bash

root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
proj=$root/../../

goversion=1.8.3
gotarget=`uname -s | awk '{print tolower($0)}'`
gobundle=go${goversion}.${gotarget}-amd64.tar.gz
gourl=https://storage.googleapis.com/golang/$gobundle
GOROOT=$root/.go
GOPATH=$root/.godev

# Download Go
if [ ! -f $GOROOT/$gobundle ]; then
    rm -rf $GOROOT
    mkdir -p $GOROOT
    curl -o $GOROOT/$gobundle $gourl
    tar -C $GOROOT -xzf $GOROOT/$gobundle
fi

export GOROOT=$GOROOT/go
export PATH=$GOROOT/bin:$PATH
export GOPATH

# Clean up previous build
rm -rf $GOPATH
mkdir -p $GOPATH

# Download dependencies
go get -u github.com/alecthomas/units
go get -u github.com/davecgh/go-spew
go get -u gopkg.in/yaml.v2

src=$GOPATH/src/code.arm.gov/dataflow
mkdir -p $src
ln -s $proj $src/sts

$root/build.sh
go test ../../...

# Copy conf files
mkdir -p $GOPATH/conf
cp $root/../dist.arm.yaml $GOPATH/conf/sts.yaml.example
cp $root/stsd.arm.dist $GOPATH/conf/stsd.dist
cp $root/stsd.service.arm.dist $GOPATH/conf/stsd.service.dist
