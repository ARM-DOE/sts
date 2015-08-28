if [ -d out ]
  then
  echo "--Cleaning old build"
  rm -r out
fi
echo "--Setting up out dir"
#mk dirs
mkdir out
mkdir out/sender
mkdir out/receiver
mkdir out/sender/bins
mkdir out/sender/test_dir
mkdir out/sender/watch_directory
mkdir out/receiver/test_dir
#copy files
cp conf/sender_config.yaml out/sender
cp conf/receiver_config.yaml out/receiver

echo "--Building Dependencies"
go get gopkg.in/yaml.v2
go get util
echo "--Building sender"
go build -o out/sender/sender src/sender/main.go src/sender/cache.go src/sender/bin.go src/sender/webserver.go src/sender/sender.go
echo "--Building receiver"
go build -o out/receiver/receiver src/receiver/main.go
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

# cleanup after testing
rm -r out/sender/test_dir
rm -r out/receiver/test_dir