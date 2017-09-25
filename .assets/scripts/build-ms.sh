#!/bin/bash

root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

GOOS=windows GOARCH=386 $root/build.sh
cp $GOPATH/bin/sts $root/sts-32.exe

GOOS=windows GOARCH=amd64 $root/build.sh
cp $GOPATH/bin/sts $root/sts-64.exe

zip ~/sts-ms.zip sts-32.exe sts-64.exe
rm sts*.exe
