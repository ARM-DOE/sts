export GOPATH=$PWD
echo "--Building dependencies"
go get gopkg.in/yaml.v2
go get util
echo "--Building sender"
go build -o src/sender/sender src/sender/main.go src/sender/cache.go src/sender/bin.go src/sender/webserver.go src/sender/sender.go
echo "--Building receiver"
go build -o src/receiver/receiver src/receiver/main.go
echo "--Testing"
go test -cover util
go test -cover sender
go test -cover receiver