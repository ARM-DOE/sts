Site Transfer Software (STS)
----------------------------

### Key Features

- Sending of one file in multiple requests
- Sending of multiple files in one request
- Safety - can crash or lose connection at any time without data loss
- Sender and receiver both contained in one binary
- Web interface config editor
- Graceful restarting on config change

### Definitions

**Go** (aka **Golang**)
  > An open-source [programming language](https://golang.org/) developed by Google.  STS is written solely in Go and compiled to a single executable.

**Thread**
  > A [Go routine](https://gobyexample.com/goroutines).

**File Watcher**
  > A thread that recursively scans the configured directory looking for new or updated files.

**Queue Cache**
  > A data store (currently kept in memory and occasionally cached to disk) to manage files to be sent.  It contains the path, size, and percentage currently allocated to bins.

**Bin**
  > A bin is the payload of data pushed via HTTP to the configured receiver.  A bin is composed of N files or file parts up to the configured bin size.  Information about what is in a particular bin (not the data itself) is recorded in a cached bin file written to the bin store.

**Bin Store**
  > Configured directory where bin metadata are cached to disk.  The name of each bin file corresponds to the MD5 hash of its contents.

**Stage Area**
  > Configured directory where files are received and reconstructed before being moved to the final configured location.

### Start-up

If STS is configured to be a sender, two threads are started for managing the outgoing flow: **File Watcher** and **Outgoing Manager**.  A similar set of threads will exist for any additionally configured target host.  Each target has its own configuration block.  STS does not currently support the sending of files from a single source directory to multiple targets.  In send mode STS will also use a configurable number of threads used for actually making the HTTP requests to the configured target.

STS starts a separate thread that acts as the **Web Server** listening on the configured port.  It is meant to act as the reception point for both send and receive incoming messages but only actively serves for both if configured to do so.

If STS is configured to run as a receiver, an additional **File Watcher** thread for watching the stage directory is started.

### Logical Flow

1. Source **File Watcher**: File found in configured watch area and added (path, size) to the outgoing queue.

  > If the queue file becomes corrupted or if the program crashes unexpectedly, the worst that can happen is the sending of duplicate data, which is obviously preferable to data loss.
  
  > The software is designed such that other technologies could be swapped in for the flat JSON file (e.g. [Redis](http://redis.io/)), which might provide better reliability at the expense of an additional dependency.

1. Source **Outgoing Manager**: Based on configured priority and tagging, files and parts of files are added to bins and bin metadata is cached to disk.
  
  > The number of bins that exist at a time corresponds to the number of configured sending threads plus a buffer.

1. **Source Sender**: When a sender thread is ready for another bin it gets the next available one from the bin store by asking the Outgoing Manager.

  > A sender thread will only work on a given bin for a configured amount of time before it gives up.  The Outgoing Manager also considers a bin recyclable based on this interval.

1. Target **Web Server**: Receives bin from source host and writes data and companion metadata file to configured stage area.

  > Each file is given a specific extension to indicate the file is not yet complete.  Once the last bin is written (Mutex locks are used to avoid conflict by multiple threads) the file is renamed to remove the previously added extension.

1. Target **Web Server** (?): Validates that written file chunk matches its MD5.  If matched nothing happens unless it is the last bin in which case a validation confirmation request is sent to the source Web Server.  Otherwise it will send a bin invalidation request to the source Web Server.

1. Source **Web Server**: Both validation and invalidation requests are passed to the Outgoing Manager.

1. Source **Outgoing Manager**: Removes validated bins from the bin store and updates the queue.

1. Source **Outgoing Manager**: For validated files, queue is updated (entry removed) and file deleted from disk (if configured to do so).  **Q:** if not configured to remove files does the cache never get flushed?

1. Source **Outgoing Manager**: For invalidated bins, the bin is made available to the next avaiable sending thread.


---


![Flowchart2](conf/sts-flow.png?raw=true)
