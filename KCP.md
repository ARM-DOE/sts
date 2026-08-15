# HTTP over KCP

STS can carry its existing HTTP/1.1 application protocol over KCP reliable UDP sessions. This mode is intended for controlled high-bandwidth, high-latency paths where TCP or HTTP/3 does not fill the available bandwidth.

The stack is:

```text
STS HTTP/1.1 -> optional TLS -> KCP reliable stream -> UDP
```

All existing STS routes, source/key authorization, payload recovery, validation, and staging behavior are retained. The normal TCP HTTP server continues to run alongside the KCP listener.

## Client configuration

```yaml
OUT:
  sources:
    - name: long-haul-source
      threads: 8
      timeout: 30m
      stat-interval: 5m
      target:
        http-host: receiver.example.org:1992
        http-tls-cert: conf/server.pem
        protocol: kcp-http
        kcp:
          tuning: balanced
```

`balanced` is the default tuning profile and may be omitted. `kcp.host` defaults to the hostname in `http-host`, and the KCP port defaults to `1993`. Existing configurations without `protocol: kcp-http` are unchanged.

## Server configuration

```yaml
IN:
  server:
    http-host: 0.0.0.0
    http-port: 1992
    http-tls-cert: conf/server.pem
    http-tls-key: conf/server.key
    kcp-enabled: true
    kcp:
      tuning: balanced
```

The client and server must use the same FEC shard settings. Permit UDP traffic to `kcp.port` in both host and network firewalls.

Enabling KCP on the server adds a parallel UDP listener; it does not replace the existing HTTP/HTTPS listener. Clients using `protocol: http`, `https`, `http3`, or `auto` continue using their existing transport.

## Tuning profiles

`balanced` applies tested, conservative defaults while still allowing individual overrides. It is intended for normal operation and is not adaptive.

`manual` makes it explicit that the operator is managing the advanced values. It currently uses the same defaults for omitted fields.

`auto` is reserved for future adaptive tuning and is rejected by configuration validation. Adaptive tuning will be added only after production and benchmark metrics provide enough evidence to distinguish network limits from CPU, compression, disk, or receiver backpressure.

### Balanced defaults

| Setting | Default |
| --- | ---: |
| `tuning` | `balanced` |
| `port` | `1993` |
| `mtu` | `1350` bytes |
| `send-window` | `4096` packets |
| `receive-window` | `4096` packets |
| `fec-enabled` | `true` |
| `data-shards` | `20` |
| `parity-shards` | `2` |
| `no-delay` | `true` |
| `interval` | `20ms` |
| `fast-resend` | `2` |
| `disable-congestion-control` | `false` |
| `ack-no-delay` | `true` |
| socket buffers | `64MB` |
| `idle-timeout` | `2m` |

### Manual overrides

```yaml
kcp:
  tuning: manual
  host: receiver.example.org
  port: 1993
  mtu: 1350
  send-window: 16384
  receive-window: 16384
  fec-enabled: true
  data-shards: 20
  parity-shards: 2
  no-delay: true
  interval: 20ms
  fast-resend: 2
  disable-congestion-control: false
  ack-no-delay: true
  socket-read-buffer: 64MB
  socket-write-buffer: 64MB
  idle-timeout: 2m
```

Set `fec-enabled: false` on both peers to benchmark without forward error correction. With FEC enabled, approximate parity overhead is `parity-shards / data-shards`; `20/2` adds approximately 10% before retransmissions and other protocol overhead.

## Window sizing

Size each KCP window to at least the bandwidth-delay product expressed in effective KCP payload packets:

```text
window packets >= target bytes/second * RTT seconds / effective bytes/packet
```

For example, 1 Gbit/s over 150 ms has an 18.75 MB bandwidth-delay product. At roughly 1300 effective payload bytes per packet, a window near 14,500 packets is required, making `16384` a reasonable starting point. KCP windows are limited to 65,535 packets.

The operating system may cap UDP socket buffers below the requested values. Verify the effective limits on production hosts and increase the relevant kernel limits when necessary.

## Session metrics

The sender logs KCP session metrics at the source's existing `stat-interval`; the normal STS default is five minutes. Metrics are emitted only while at least one KCP session is active and include:

- Active and sampled session counts
- Average and maximum smoothed RTT
- Average and maximum RTT variance
- Average and maximum retransmission timeout

Useful request throughput continues to be logged at the same interval by the existing bandwidth monitor. Use `stat-interval: 10s` to `30s` for short controlled benchmarks. For normal operation, keep the five-minute default to reveal trends without producing excessive logs.

Retransmission, inferred-loss, and FEC-recovery counters in `kcp-go` are process-global rather than session-local. They are not currently labeled as per-source metrics; a process-level collector is the next prerequisite for adaptive `auto` tuning.

## Security

When the existing HTTP TLS certificate settings are present, TLS is carried inside KCP and continues to provide server authentication plus application-data confidentiality and integrity. STS currently uses KCP's no-op packet codec, so KCP headers are not cryptographically authenticated. On-path modification can disrupt a session but cannot create valid TLS application data.

Use this mode on a trusted network or VPN. Adding optional KCP-layer AES-GCM authentication can be considered separately if transport-header tampering is in scope.

## Rollout and measurement

KCP selection is static; there is no automatic mid-request fallback. If a KCP upload fails, existing STS partial-transfer recovery applies on the next attempt.

Before rollout, compare TCP/HTTPS, HTTP/3, and KCP/HTTPS with the same payload size and concurrency under representative bandwidth, RTT, jitter, and packet loss. At minimum, test FEC settings `off`, `20/2`, and `10/3`. Do not enable `disable-congestion-control` on a shared network without controlled testing.
