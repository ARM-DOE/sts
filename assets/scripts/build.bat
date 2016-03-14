@echo off
SET GOPATH=%CD%
if exist out\sender\sts.exe (
	echo --Cleaning old build
	del out\sender\sts.exe
	del out\receiver\sts.exe
	rd /s /q pkg
)
echo --Setting up out dir
mkdir out\sender\bins >nul 2>&1
mkdir out\sender\test_dir >nul 2>&1
mkdir out\sender\watch_directory >nul 2>&1
mkdir out\receiver\test_dir >nul 2>&1
echo --Building Dependencies
go get gopkg.in/yaml.v2
go get util
echo --Building sender
go get sender
echo --Building receiver
go get receiver

go build -o out\sender\sts.exe src\sts\sts.go
copy out\sender\sts.exe out\receiver\sts.exe >nul
rd /s /q out\sender\test_dir
rd /s /q out\receiver\test_dir

:: usage from out directory:
:: sts.exe -s ../../conf/sender_config.yaml
:: sts.exe -r ../../conf/receiver_config.yaml