if [ -f out/sender/sts ]
  then
  echo "--Cleaning old build"
  rm out/sender/sts
  rm out/receiver/sts
  rm -r pkg
fi
echo "--Setting up out dir"
#mk dirs
mkdir -p out/sender/bins
mkdir -p out/sender/test_dir
mkdir -p out/sender/watch_directory
mkdir -p out/receiver/test_dir
echo "--Building Dependencies"
go get gopkg.in/yaml.v2
go get util
echo "--Building sender"
go get sender
echo "--Building receiver"
go get receiver
# generate binary
go build -o out/sender/sts src/sts/sts.go
cp out/sender/sts out/receiver/sts
sts_alive="$(pgrep sts)"
if [[ $sts_alive ]] # do not run tests if either the sender or receiver are currently running
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

# usage from out directory:
# ./sts -s ../../conf/sender_config.yaml
# ./sts -r ../../conf/receiver_config.yaml