# HTTP3/QUIC Support in STS

## Overview

STS now supports **HTTP3 over QUIC** for improved performance in high-latency networks. HTTP3 provides:

- **Faster Connection Setup**: 1-RTT handshake vs 2-3 RTT for HTTPS/TCP
- **No Head-of-Line Blocking**: Independent stream processing
- **Better Packet Loss Handling**: Individual stream retransmission
- **Connection Migration**: Seamless handover during IP changes
- **0-RTT Resumption**: Near-instant reconnection for repeat transfers

## Quick Start

### Server Configuration

Enable HTTP3 on your server by adding these fields to your configuration:

```yaml
IN:
  server:
    http-port: 443
    http-tls-cert: conf/server.pem
    http-tls-key: conf/server.key

    # Enable HTTP3
    http3-enabled: true

    # Optional: Advanced QUIC tuning
    quic-max-streams: 100
    quic-max-idle-timeout: 30s
    quic-keep-alive: 15s
```

### Client Configuration

Configure clients to use HTTP3 with automatic fallback:

```yaml
OUT:
  sources:
    - name: my-source
      target:
        http-host: server.example.com:443
        http-tls-cert: conf/server.pem

        # Use HTTP3 with automatic fallback to HTTPS
        protocol: auto
```

## Protocol Selection

The `protocol` field supports these values:

| Value | Behavior |
| ----- | -------- |
| `auto` | Try HTTP3 first, automatically fall back to HTTPS if HTTP3 fails |
| `http3` | Use HTTP3 only (fails if HTTP3 unavailable) |
| `https` | Use HTTPS/HTTP2 only (traditional behavior) |
| `http` | Use HTTP without TLS (not recommended for production) |

**Recommended**: Use `protocol: auto` for gradual migration and resilience.

## Network Requirements

### Firewall Configuration

HTTP3 uses **UDP** instead of TCP. Ensure your firewall allows:

- **Outbound**: UDP port 443 (or your configured port)
- **Inbound** (server): UDP port 443 (or your configured port)

Example iptables rule:

```bash
# Allow UDP port 443 for HTTP3
iptables -A INPUT -p udp --dport 443 -j ACCEPT
iptables -A OUTPUT -p udp --sport 443 -j ACCEPT
```

### Port Configuration

By default, HTTP3 and HTTPS share the same port (443). You can optionally use separate ports:

```yaml
# Server
server:
  http-port: 443      # HTTPS port
  http3-port: 4433    # Separate HTTP3 port

# Client
target:
  http-host: server.example.com:443
  http3-port: 4433
```

## Performance Tuning

### QUIC Stream Configuration

Adjust concurrent stream limits based on your workload:

```yaml
server:
  # More streams = more concurrent file transfers
  quic-max-streams: 200  # Default: 100
```

### Timeout Configuration

For high-latency networks, increase timeouts:

```yaml
server:
  quic-max-idle-timeout: 60s  # Default: 30s
  quic-keep-alive: 20s        # Default: 15s
```

### Client-Side Timeout

Ensure client timeout is longer than server idle timeout:

```yaml
sources:
  - timeout: 90s  # Should be > quic-max-idle-timeout
```

## Monitoring & Debugging

### Verify HTTP3 is Active

Check logs for HTTP3 initialization:

```text
INFO: Starting HTTP3/QUIC server on 0.0.0.0:443
INFO: (my-source) Protocol: HTTP3/QUIC (with HTTPS fallback)
```

### Fallback Events

If HTTP3 fails, you'll see fallback messages:

```text
INFO: (my-source) HTTP3 connection failed (QUIC handshake timeout), falling back to HTTPS
```

### Network Analysis

Use Wireshark to inspect QUIC traffic:

1. Filter: `udp.port == 443`
2. Look for QUIC handshake packets
3. Verify ALPN negotiation shows "h3"

## Migration Strategy

### Phase 1: Server Upgrade (No Disruption)

1. Enable HTTP3 on server:

   ```yaml
   server:
     http3-enabled: true
   ```

2. Restart server - both HTTP/1.1 and HTTP3 now available
3. Existing clients continue using HTTPS without changes

### Phase 2: Gradual Client Rollout

1. Update 10% of clients to use `protocol: auto`
2. Monitor for:
   - Fallback rate
   - Performance improvements
   - Error rates

3. Gradually increase to 50%, then 100%

### Phase 3: Optimization

1. Analyze actual usage patterns
2. Tune QUIC parameters based on real-world data
3. Consider forcing HTTP3 for specific high-latency routes:

   ```yaml
   protocol: http3  # No fallback
   ```

## Troubleshooting

### HTTP3 Not Working

**Symptom**: Client always falls back to HTTPS

**Checks**:

1. ✅ Server has `http3-enabled: true`
2. ✅ Firewall allows UDP traffic
3. ✅ Client has TLS configured (HTTP3 requires TLS)
4. ✅ Server certificate is valid
5. ✅ No UDP-blocking middleboxes (some corporate networks block UDP)

**Test UDP connectivity**:

```bash
# On client
nc -u server.example.com 443
```

### High Fallback Rate

If >20% of connections fall back to HTTPS:

1. **Check Network Path**: Some networks block UDP
2. **Verify MTU**: QUIC sensitive to MTU issues

   ```bash
   # Test MTU
   ping -M do -s 1400 server.example.com
   ```

3. **Check Server Load**: Server may be rejecting connections

### Performance Worse Than HTTPS

Possible causes:

1. **Low Packet Loss**: HTTP3 benefits most from >1% packet loss
2. **Low Latency**: HTTP3 overhead may hurt on LAN (<10ms RTT)
3. **CPU Constraints**: QUIC encryption is CPU-intensive

**Solution**: Use `protocol: https` for low-latency, reliable networks

## Advanced Features

### 0-RTT Resumption

Automatically enabled for repeat connections. Reduces connection time to ~0ms.

**Security Note**: 0-RTT can be vulnerable to replay attacks. STS mitigates this by:

- Only using 0-RTT for idempotent operations
- Server-side replay protection

### Connection Migration

If client IP changes (e.g., mobile network handoff), QUIC seamlessly migrates the connection without re-handshake.

## Configuration Reference

### Client Parameters

```yaml
target:
  protocol: auto              # auto | http3 | https | http
  http3-port: 0               # 0 = use same port as http-host
```

### Server Parameters

```yaml
server:
  http3-enabled: true         # Enable HTTP3 server
  http3-port: 0               # 0 = use same port as http-port
  quic-max-streams: 100       # Max concurrent streams per connection
  quic-max-idle-timeout: 30s  # Connection idle timeout
  quic-keep-alive: 15s        # Keep-alive interval
```

## Performance Expectations

Expected improvements in high-latency environments:

| Metric | HTTP/1.1 | HTTP3 | Improvement |
| ------ | -------- | ----- | ----------- |
| Connection setup (200ms RTT) | ~600ms | ~200ms | **3x faster** |
| Throughput with 1% packet loss | 50% degradation | 10% degradation | **5x better** |
| File transfer with concurrent streams | Sequential blocking | Parallel processing | **No blocking** |
| Connection migration latency | Full reconnect (~1s) | Seamless (0ms) | **Infinite** |

## Compatibility

- **Go Version**: Requires Go 1.21+
- **TLS Version**: Requires TLS 1.3 (automatically configured)
- **OS Support**: Linux, macOS, Windows (via UDP stack)
- **Network**: Requires UDP port accessibility

## Further Reading

- [HTTP/3 RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html)
- [QUIC RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html)
- [quic-go Documentation](https://github.com/quic-go/quic-go)
