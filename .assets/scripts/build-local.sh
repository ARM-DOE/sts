#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJ=$ROOT/../../

GOVERSION=1.8.3
GOTARGET=`uname -s | awk '{print tolower($0)}'`
GOBUNDLE=go${GOVERSION}.${GOTARGET}-amd64.tar.gz
GOURL=https://storage.googleapis.com/golang/$GOBUNDLE
GOROOT=$ROOT/.go
GOPATH=$ROOT/.godev

# Download Go
if [ ! -f $GOROOT/$GOBUNDLE ]; then
    rm -rf $GOROOT
    mkdir -p $GOROOT
    curl -o $GOROOT/$GOBUNDLE $GOURL
    tar -C $GOROOT -xzf $GOROOT/$GOBUNDLE
fi

export GOROOT=$GOROOT/go
export PATH=$GOROOT/bin:$PATH
export GOPATH

# Clean up previous build
rm -rf $GOPATH
mkdir -p $GOPATH

# Download dependencies
git clone https://github.com/alecthomas/units $GOPATH/src/github.com/alecthomas/units
git clone https://github.com/davecgh/go-spew $GOPATH/src/github.com/davecgh/go-spew
git clone https://github.com/go-yaml/yaml.git $GOPATH/src/gopkg.in/yaml.v2
cd $GOPATH/src/gopkg.in/yaml.v2 ; git checkout v2; cd $ROOT

src=$GOPATH/src/code.arm.gov/dataflow
mkdir -p $src
ln -s $PROJ $src/sts

$ROOT/build.sh
go test ../../...

# Copy conf files
mkdir -p $GOPATH/conf
cp $ROOT/../dist.arm.yaml $GOPATH/conf/sts.yaml.example
cp $ROOT/stsd.arm.dist $GOPATH/conf/stsd.dist
cp $ROOT/stsd.service.arm.dist $GOPATH/conf/stsd.service.dist
