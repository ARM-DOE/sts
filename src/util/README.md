
About:
----------------------------

`util` is a library of code that is required by both the sender and the receiver program.

----------------------------
File           | Description
---------------|--------------------
`companion.go` | Handles companion files written to disk, used for keeping track of how many parts of a file have been received.
`config.go`    |Used for parsing and editing config files safely.
`listener.go`  |Tool to scan a directory via polling and send new files to a channel. Uses a cache file which can store arbitrary data about detected files.
`logger.go`    |Code for logging different events. (errors, successful send/receive)
`md5.go`       |Code for generating md5s of byte arrays or files without reading whole file into memory.
`tls.go`       |Implementation of a gracefully interruptible TLS server & TLS HTTP client.
`util.go`      |Miscellaneous utility functions.