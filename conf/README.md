Source (Sender) Configuration
-----------------------------

Full path to the root directory.  Directory structure beyond the root will be replicated on the target.
```yaml
input_directory: /full/path/to/data/root
```

Full path to the cache file used for outgoing file queue.
```yaml
cache_file_name: /full/path/to/file_cache.dat
```

Full path to root of logs directory.  Log files will be written in this form:

```yaml
logs_directory: /full/path/to/logs/root
```

e.g. `<logs_directory>/outgoing_to/<host>/<yyyy><mm>/<dd>.log`

Full path to bin store directory.
```yaml
bin_store: bins
```

Number of threads sending data concurrently.
```yaml
sender_threads: 10
```

How often to cache the file queue to disk (in seconds).
```yaml
cache_write_interval: 60
```

How long is too long for a bin to be worked on by a single thread (in seconds).
```yaml
bin_timeout: 3600
```

Target host name/IP and port.
```yaml
receiver_address: c1.dmf.arm.gov:8000
```

Use HTTP over TLS.  Specify certificate parameters if applicable.
```yaml
tls: false
client_ssl_cert: ../../conf/client.pem
client_ssl_key: ../../conf/client.key
```

Everything under `dynamic` represents configuration that can change without requiring a server restart.

The `bin_size` is the target number of bytes of data to be sent in a single HTTP request.

When `compression` set to `true` the data payload is gzipped.

Under `tags` are keys that represent a tag pattern (except `DEFAULT` which houses the default parameters not overriden).
A tag is simply the part of the file path between the outgoing root and the first dot.

Tags with a lower `priority` integer value have higher priority.

Tags with a `sort` value of  anything (e.g. `modtime`) other than `none` will be received in order.

The `transfer_method` is limited right now to `HTTP` and `disk` but could be expanded to other methods as needed.
```yaml
dynamic:
  bin_size: 300000
  compression: false
  tags:
    DEFAULT:
      priority: 5
      sort: modtime
      delete_on_verify: false
      transfer_method: HTTP
    lowpriority:
      priority: 10
```


Target (Receiver) Configuration
-------------------------------

Full path to the cache file used for outgoing file queue.
```yaml
cache_file_name: listener_cache.dat
```

How often to cache the file queue to disk (in seconds).
```yaml
cache_write_interval: 60
```

Full path to the directory where files are initially received and reassembled.  The sending `hostname` will be appended to avoid collisions.
```yaml
staging_directory: /full/path/to/data/root
```

Full path to the directory where files are moved after validation.  The sending `hostname` will be appended to avoid collisions.
```yaml
output_directory: /full/path/to/output/root
```

Full path to root of logs directory.

```yaml
logs_directory: /full/path/to/logs/root
```
e.g. `<logs_directory>/incoming_from/<host>/<yyyy><mm>/<dd>.log`

Port to listen for HTTP requests on.
```yaml
server_port: 8081
```

Use HTTP over TLS.  Specify certificate parameters if applicable.
```yaml
tls: false
server_ssl_cert: ../../conf/server.pem
server_ssl_key: ../../conf/server.key
```
