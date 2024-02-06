#!/bin/bash

root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
proj=$root/../../

goversion=1.21.6
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
chmod -R u+w $GOPATH  # Directories with 555 perms make cleanup difficult
rm -rf $GOPATH
mkdir -p $GOPATH

src=$GOPATH/src/code.arm.gov/dataflow
mkdir -p $src
ln -s $proj $src/sts

# Build
$root/build.sh
if [ ! -f $GOPATH/bin/sts ]; then
    echo "Build failed"
    exit 1
fi

# Run all tests
cd $root; go test ../../...

# Copy conf files
mkdir -p $GOPATH/conf
cp $root/../dist.arm.yaml $GOPATH/conf/sts.yaml.example
cp $root/stsd.service $GOPATH/conf
