export GOPATH=$PWD

if [ -f src/sender/sender ] # if binary exists, clean the old build
  then
    echo "--Cleaning old build"
    go clean -i receiver
    go clean -i sender
    go clean -i util
fi
echo "--Building dependencies"
go get gopkg.in/yaml.v2
go get util
echo "--Building sender"
go build -o src/sender/sender src/sender/main.go src/sender/cache.go src/sender/bin.go src/sender/webserver.go src/sender/sender.go
echo "--Building receiver"
go build -o src/receiver/receiver src/receiver/main.go
sender_alive="$(pgrep sender)"
receiver_alive="$(pgrep receiver)"
if [[ $sender_alive || $receiver_alive ]] # do not run tests if either the sender or receiver are currently running
  then
  	echo "--Sender/receiver running, skipping tests"
  else
    echo "--Testing"
    go test -cover util
    go test -cover sender
    go test -cover receiver
fi