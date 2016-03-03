
About:
----------------------------

`sender` is an independent Go package that contains all code for the sender portion of the STS. It is responsible for detecting files on disk, packaging them into bins, and sending them to the receiver over HTTP or writing them to disk.

----------------------------

File              | Description
------------------|-------------------
`bin.go`          | <ul><li>Creates, fills, and writes bin metadata to disk.</li><li>Determines file send order.</li></ul>
`cache.go`        | A wrapper over util.Listener that receives file paths from a Listener object, triggers bin allocation for new files, and stores file metadata (number of bytes allocated, allocation start time) in the Listener cache file.
`main.go`         | <ul><li>Entry point of the sender program.</li><li>Parses config file & sets up global variables</li><li>Initializes cache, poller, and sender threads.</li><li>Restarts program on config file changes</li></ul>
`sender.go`       | Contains file sending logic. Each sender instance consumes Bins passed down a channel and constructs the parts of the files that they represent. These file parts are either written to disk or sent to the receiver over HTTP.
`poll.go`    | A Poller instance is spawned in a separate gorountine, it polls the receiver for files which have been validated.


Use:
----------------------------

`sts -s /path/to/config.yaml`

Example config stored in conf directory.