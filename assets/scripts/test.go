package main

import (
	"bytes"
	"compress/gzip"
	"os"
)

func main() {
	buff := &bytes.Buffer{}
	fh, _ := os.OpenFile("./file.gz", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	gz, _ := gzip.NewWriterLevel(buff, gzip.BestCompression)
	buff.Reset()
	gz.Write([]byte("Testing"))
	gz.Close()
	fh.Write(buff.Bytes())
	fh.Close()
	return
}
