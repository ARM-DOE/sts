Site Transfer Software (STS)
----------------------------

STS is software used for transmitting data over wide-area networks with the following priorities:

1. In-order delivery
2. Confirmed transfer via hash validation
3. Efficient use of bandwidth using HTTP and gzip compression
4. Bandwidth sharing among configured groups of files to avoid starvation

### Usage

```
$ sts -help
  -conf string
    	Configuration file path
  -debug
    	Log program flow
  -help
    	Print the help message
  -loop
    	Run in a loop, i.e. don't exit until interrupted
  -mode string
    	Mode: "send", "receive", "auto" (default "auto")
```

### Example Configuration

Below is an example configuration file.  The "outgoing" and "incoming" blocks do not have to be included in the same configuration file.  In fact, by default if `-conf` not used, STS will look in `$STS_HOME` or `$PWD` for `sts.{mode}.yaml` (or `sts.yaml` if running `--mode=both`).

```yaml
# OUTGOING CONFIGURATION
OUT:
  dirs: # Outgoing directory configuration; relative to $STS_HOME or $PWD if not absolute
    cache : .sts     # Used to store queue cache(s)
    out   : data/out # Directory to watch for files to send; appends "/{target name}"
    logs  : data/log # For log files; appends "outgoing_to/{target name}" and "messages"
  sources: # Supports multiple sources where omitted entries will inherit from previous sources hierarchically
    - name          : ...   # Name of the source
      threads       : 8     # Maximum number of concurrent HTTP connections
      bin-size      : 10MB  # The generally-desired size for a given HTTP request (BEFORE any compression)
      compress      : true  # Use GZIP compression (NOTE: bin-size is based on file size BEFORE compression)
      min-age       : 15s   # How old a file must be before being added to the "outgoing" queue
      max-age       : 12h   # How old a file can be before getting logged as "stale" (remains in the queue)
      timeout       : 1h    # The HTTP timeout for a single request
      poll-delay    : 5s    # How long to wait after file sent before final validation
      poll-interval : 1m    # How long to wait between polling requests
      poll-attempts : 10    # How many times to "poll" for the successful reception of a file before re-sending
      target: # Target-specific configuration
        name          : ...      # Name of the target
        http-host     : ...:1992 # Target host, including port
        http-tls      : true     # Whether or not the target host uses HTTPS
        http-tls-cert : conf/client.pem  # Client certificate; relative to $STS_HOME or $PWD if not absolute
        http-tls-key  : conf/client.key  # Client key; relative to $STS_HOME or $PWD if not absolute
      tags: # Tags are for configuration based on file patterns (omitted attributes are inherited)
        - pattern   : DEFAULT # The file "tag" pattern
          priority  : 0       # Relative importance (higher the number, greater the importance)
          order     : fifo    # File order (fifo (first in, first out) or none)
          delete    : true    # Whether or not to delete files after reception confirmation
          method    : http    # Transfer method ("http", "disk", or "none")

# INCOMING CONFIGURATION
IN:
  dirs: # Incoming directory configuration; relative to $STS_HOME or $PWD if not absolute
    stage : data/stage # Directory to stage data as it comes in; appends "/{source name}"
    final : data/in    # Final location for incoming files; appends "/{source name}"
    logs  : data/log   # For log files; appends "incoming_from/{source name}" and "messages"
  server: # Server configuration.
    http-port     : 1992  # What port to listen on
    http-tls      : true  # Whether or not to use HTTPS
    http-tls-cert : conf/server.pem # Server certificate path; relative to $STS_HOME or $PWD if not absolute
    http-tls-key  : conf/server.key # Server key; relative to $STS_HOME or $PWD if not absolute
```

### Definitions

**Go** (aka **Golang**)
  > An open-source [programming language](https://golang.org/) developed by Google.  STS is written solely in Go and compiled to a single executable.

**Thread**
  > A [Go routine](https://gobyexample.com/goroutines).

**Queue Cache**
  > A data store (currently kept in memory and cached in JSON format to disk after each scan) to manage files in the outgoing queue.  It contains a file's path, size, and mod time.  Its purpose is to keep track of files found but not fully sent and validated such that on a crash recovery, files can be appropriately resent without duplication.

**Bin**
  > A bin is the payload of data pushed via HTTP to the configured target.  A bin is composed of N files or file parts up to the configured bin size (before compression).

**Stage Area**
  > Configured directory where files are received and reconstructed before being moved to the final configured location.

### Start-up

If STS is configured to send, the first thing it will do is check the cache to see if any files are already in the queue from last run.  If so, a request is made to the target to find out what "partials" exist and corresponding companion metadata is returned.  From this, STS can determine which files are partially sent already and which ones need to be polled for validation.  Some of these might have already been fully received and some not at all.  STS will handle both of these cases to avoid duplicate transfers.

Following the outgoing recovery logic, four components are started for managing the outgoing flow: **Watcher**, **Sorter**, **Sender**, and **Validator**.  A similar set of components will be started for any additionally configured source + target.  Each source + target has its own configuration block.  In send mode STS will also use a configurable number of threads used for making concurrent HTTP requests to the configured target.

If STS is configured to run as a receiver, three additional components are started: 1) **HTTP Server** for receiving files and validation requests and 2) a **Watcher** for scanning the stage directory, and 3) a **Finalizer** that takes files from the Watcher and validates them (hash match and in-order delivery) before moving them to the "final" directory.

### Logical Flow

1. _Source_ **Watcher**: Files found in configured watch directory are cached in memory (and on disk) and passed to the **Sorter**.

  > If the queue cache becomes corrupted or if the program crashes unexpectedly, STS is will perform a recovery procedure on next run that will pick up where it left off without sending duplicate data.

1. _Source_ **Sorter**: Files received from **Watcher** are sorted in order of configured priority and/or in-order delivery.  **Sorter** passes files to the **Sender** such that groups of similar files (based on configurable pattern match) of the same priority are rotated in order to avoid starvation.

1. _Source_ **Sender**: Does these activities in parallel:

  1. Compute MD5 hash of each input file
  1. Construct "bin" until full (or until input file stream is stagnant for a second)
  1. POST "bin" to target HTTP server, optionally compressing the payload
  1. Log successfully sent files and pass these to the **Validator**

1. _Source_ **Validator**: Builds a batch of files to validate based on configurable time interval and makes a request to the target host to confirm files sent have been validated and finalized.  Files that fail validation are sent back to the **Sender**.  Files that pass validation are communicated to the **Watcher** (removes from queue) and **Sorter** (removes from list and does any cleanup action configured for its group).  After a configurable number of poll attempts do not yield success, files are passed back to the **Sender**.

1. _Target_ **HTTP Server**: Receives POSTed "bin" from source host and writes data and companion metadata file to configured stage area.  Each file is given a ".part" extension to indicate the file is not yet complete.  Once the last part is written (mutex locks are used to avoid conflict by multiple threads) the file is renamed to remove the previously added ".part" extension.

1. _Target_ **Watcher**: Scans stage area for completed files and sends them to the **Finalizer**.

1. _Target_ **Finalizer**: Makes sure each input file matches the MD5 hash as stored in the "companion" file.  Also makes sure that a file is only finalized following its predecessor (if one specified in the companion file).


![Flowchart2](assets/flow.png?raw=true)
