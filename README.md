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

If STS is configured to be a sender, three threads are started for managing the outgoing flow: **File Watcher**, **Outgoing Manager**, and **Confirmation Poller**.  A similar set of threads will exist for any additionally configured target host.  Each target has its own configuration block.  STS does not currently support the sending of files from a single source directory to multiple targets.  In send mode STS will also use a configurable number of threads used for actually making the HTTP requests to the configured target.

If STS is configured to run as a receiver, two additional threads are started: 1) **HTTP Server** for receiving files and validation requests and 2) a **File Watcher** thread for watching the stage directory.

### Logical Flow

1. _Source_ **File Watcher**: Files found in configured watch area and added (path, size) to the outgoing queue.

  > If the queue file becomes corrupted or if the program crashes unexpectedly, the worst that can happen is the sending of duplicate data, which is obviously preferable to data loss.
  
  > The software is designed such that other technologies could be swapped in for the flat JSON file (e.g. [Redis](http://redis.io/)), which might provide better reliability at the expense of an additional dependency.

1. _Source_ **Outgoing Manager**: Based on configured priority and tagging, files or parts of files are added to bins and bin metadata is cached to the bin store.
  
  > The number of bins that exist at a time corresponds to the number of configured sending threads plus a buffer.

1. _Source_ **Sender**: When a sender thread is ready for another bin it gets the next available one from the bin store by asking the Outgoing Manager.  It then POSTs the compressed bin to the target HTTP server.

  > A sender thread will only work on a given bin for a configured amount of time before it gives up.  The Outgoing Manager also considers a bin recyclable based on this interval.

1. _Target_ **HTTP Server**: Receives bin from source host and writes data and companion metadata file to configured stage area.

  > Each file is given a specific extension to indicate the file is not yet complete.  Once the last bin is written (Mutex locks are used to avoid conflict by multiple threads) the file is renamed to remove the previously added extension.

1. _Target_ **HTTP Server**: Sends validation as response to initial bin POST request.

1. _Source_ **Sender**: Removes validated bin from the bin store and updates the queue.

1. _Target_ **File Watcher**: Watches for completed file and validates against its MD5.  After validation, the file is renamed into the final destination and then logged.

  > An important note here is that files have to be renamed (moved) in the proper sorted order as determined by the Source Sender.  To accomplish this, a custom HTTP header attribute is used by the sender to indicate which file it must follow.  The Target File Watcher is responsible to make sure that a file gets moved only after its predecessor, if applicable, has been.
  
1. _Source_ **Confirmation Poller**: When all parts of a file have been sent, the poller sends regular requests to the Target HTTP Server to confirm the file was received and validated.

  > If, after some amount of time, no validation is confirmed then the file will be put back on the queue.

1. _Source_ **Outgoing Manager**: For validated files, queue is updated (entry removed) and file deleted from disk (if configured to do so).

1. _Source_ **Outgoing Manager**: For invalidated bins, the bin is made available to the next avaiable sending thread to be resent.


![Flowchart2](conf/sts-flow.png?raw=true)
