export GOPATH=$PWD/lib
echo "--Building dependencies"
go get gopkg.in/yaml.v2
go get util
echo "--Building sender"
go build -o sender/sender sender/main.go sender/cache.go sender/bin.go sender/webserver.go sender/sender.go
echo "--Building receiver"
go build -o receiver/receiver receiver/main.go