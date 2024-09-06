#!/bin/bash

root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$root/build.sh --os=windows --arch=386
cp $GOPATH/bin/sts.exe $root/sts-32.exe

$root/build.sh --os=windows --arch=amd64
cp $GOPATH/bin/sts.exe $root/sts-64.exe

zip ~/sts-ms.zip sts-32.exe sts-64.exe
rm sts*.exe
