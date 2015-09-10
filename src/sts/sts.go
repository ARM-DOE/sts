package main

import (
    "fmt"
    "os"
    "receiver"
    "sender"
)

func main() {
    if len(os.Args) < 3 {
        help()
    }
    config := os.Args[2]
    switch os.Args[1] {
    case "-s":
        sender.Main(config)
    case "-r":
        receiver.Main(config)
    default:
        help()
    }
    return
}

func help() {
    fmt.Println("usage: sts [-r | -s] config")
    os.Exit(1)
}
