package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
	kcp "github.com/xtaci/kcp-go/v5"
)

// KCPListener applies STS KCP settings to every accepted reliable UDP session.
type KCPListener struct {
	*kcp.Listener
	conf *sts.KCPConf
}

type kcpSessionRegistry struct {
	lock     sync.RWMutex
	sessions map[*kcp.UDPSession]struct{}
}

type trackedKCPSession struct {
	*kcp.UDPSession
	registry *kcpSessionRegistry
	once     sync.Once
}

type kcpSessionMetrics struct {
	Active        int
	Samples       int
	AverageSRTT   time.Duration
	MaximumSRTT   time.Duration
	AverageRTTVar time.Duration
	MaximumRTTVar time.Duration
	AverageRTO    time.Duration
	MaximumRTO    time.Duration
}

func newKCPSessionRegistry() *kcpSessionRegistry {
	return &kcpSessionRegistry{sessions: make(map[*kcp.UDPSession]struct{})}
}

func (r *kcpSessionRegistry) track(session *kcp.UDPSession) net.Conn {
	r.lock.Lock()
	r.sessions[session] = struct{}{}
	r.lock.Unlock()
	return &trackedKCPSession{UDPSession: session, registry: r}
}

func (s *trackedKCPSession) Close() error {
	s.once.Do(func() {
		s.registry.lock.Lock()
		delete(s.registry.sessions, s.UDPSession)
		s.registry.lock.Unlock()
	})
	return s.UDPSession.Close()
}

func (r *kcpSessionRegistry) snapshot() kcpSessionMetrics {
	r.lock.RLock()
	defer r.lock.RUnlock()

	metrics := kcpSessionMetrics{Active: len(r.sessions)}
	var totalSRTT time.Duration
	var totalRTTVar time.Duration
	var totalRTO time.Duration
	for session := range r.sessions {
		srtt := time.Duration(session.GetSRTT()) * time.Millisecond
		if srtt <= 0 {
			continue
		}
		rttVar := time.Duration(session.GetSRTTVar()) * time.Millisecond
		rto := time.Duration(session.GetRTO()) * time.Millisecond
		metrics.Samples++
		totalSRTT += srtt
		totalRTTVar += rttVar
		totalRTO += rto
		if srtt > metrics.MaximumSRTT {
			metrics.MaximumSRTT = srtt
		}
		if rttVar > metrics.MaximumRTTVar {
			metrics.MaximumRTTVar = rttVar
		}
		if rto > metrics.MaximumRTO {
			metrics.MaximumRTO = rto
		}
	}
	if metrics.Samples > 0 {
		count := time.Duration(metrics.Samples)
		metrics.AverageSRTT = totalSRTT / count
		metrics.AverageRTTVar = totalRTTVar / count
		metrics.AverageRTO = totalRTO / count
	}
	return metrics
}

func (r *kcpSessionRegistry) log(interval time.Duration, prefix string, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metrics := r.snapshot()
			if metrics.Active == 0 {
				continue
			}
			log.Info(fmt.Sprintf(
				"%sKCP session metrics (interval %s): active=%d sampled=%d srtt_avg=%s srtt_max=%s rttvar_avg=%s rttvar_max=%s rto_avg=%s rto_max=%s",
				prefix, interval, metrics.Active, metrics.Samples,
				metrics.AverageSRTT, metrics.MaximumSRTT,
				metrics.AverageRTTVar, metrics.MaximumRTTVar,
				metrics.AverageRTO, metrics.MaximumRTO,
			))
		case <-stop:
			return
		}
	}
}

// ListenKCP creates a net.Listener backed by KCP over UDP.
func ListenKCP(addr string, conf *sts.KCPConf) (*KCPListener, error) {
	if conf == nil {
		return nil, fmt.Errorf("KCP configuration is required")
	}
	block, err := kcp.NewNoneBlockCrypt(nil)
	if err != nil {
		return nil, fmt.Errorf("create KCP packet codec: %w", err)
	}
	listener, err := kcp.ListenWithOptions(addr, block, conf.DataShards, conf.ParityShards)
	if err != nil {
		return nil, err
	}
	if err = listener.SetReadBuffer(int(conf.SocketReadBuffer)); err != nil {
		listener.Close()
		return nil, fmt.Errorf("set KCP socket read buffer: %w", err)
	}
	if err = listener.SetWriteBuffer(int(conf.SocketWriteBuffer)); err != nil {
		listener.Close()
		return nil, fmt.Errorf("set KCP socket write buffer: %w", err)
	}
	return &KCPListener{Listener: listener, conf: conf}, nil
}

func (l *KCPListener) Accept() (net.Conn, error) {
	session, err := l.AcceptKCP()
	if err != nil {
		return nil, err
	}
	configureKCPSession(session, l.conf)
	return session, nil
}

func configureKCPSession(session *kcp.UDPSession, conf *sts.KCPConf) {
	noDelay := 0
	if conf.NoDelay {
		noDelay = 1
	}
	congestionControl := 0
	if conf.DisableCongestionControl {
		congestionControl = 1
	}
	session.SetNoDelay(noDelay, int(conf.Interval/time.Millisecond), conf.FastResend, congestionControl)
	session.SetWindowSize(conf.SendWindow, conf.ReceiveWindow)
	session.SetMtu(conf.MTU)
	session.SetACKNoDelay(conf.ACKNoDelay)
	session.SetWriteDelay(false)
}

func dialKCPContext(ctx context.Context, conf *sts.KCPConf, address string, registry *kcpSessionRegistry) (net.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	block, err := kcp.NewNoneBlockCrypt(nil)
	if err != nil {
		return nil, fmt.Errorf("create KCP packet codec: %w", err)
	}
	session, err := kcp.DialWithOptions(address, block, conf.DataShards, conf.ParityShards)
	if err != nil {
		return nil, err
	}
	configureKCPSession(session, conf)
	if err = session.SetReadBuffer(int(conf.SocketReadBuffer)); err != nil {
		session.Close()
		return nil, fmt.Errorf("set KCP socket read buffer: %w", err)
	}
	if err = session.SetWriteBuffer(int(conf.SocketWriteBuffer)); err != nil {
		session.Close()
		return nil, fmt.Errorf("set KCP socket write buffer: %w", err)
	}
	return registry.track(session), nil
}

func (tb *TransportBuilder) buildKCPHTTPClient() (*BandwidthLoggingClient, error) {
	if tb.KCPConfig == nil {
		return nil, fmt.Errorf("KCP configuration is required")
	}
	maxConnections := tb.MaxConnections
	if maxConnections < 1 {
		maxConnections = 1
	}
	address := net.JoinHostPort(tb.KCPConfig.Host, strconv.Itoa(tb.KCPConfig.Port))
	sessions := newKCPSessionRegistry()
	transport := &http.Transport{
		TLSClientConfig:     tb.TLSConfig,
		DisableKeepAlives:   false,
		MaxIdleConns:        maxConnections,
		MaxIdleConnsPerHost: maxConnections,
		MaxConnsPerHost:     maxConnections,
		IdleConnTimeout:     tb.KCPConfig.IdleTimeout,
		ForceAttemptHTTP2:   false,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialKCPContext(ctx, tb.KCPConfig, address, sessions)
		},
	}
	if tb.TLSConfig != nil {
		transport.TLSClientConfig = tb.TLSConfig.Clone()
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	monitor := &bandwidthMonitorTransport{transport: transport}
	client := newBandwidthLoggingClient(monitor, tb.LogInterval, func(i ...interface{}) {
		log.Info(append([]interface{}{tb.LogPrefix}, i...)...)
	})
	client.kcpSessions = sessions
	if tb.LogInterval > 0 {
		go sessions.log(tb.LogInterval, tb.LogPrefix, client.stopChan)
	}
	return client, nil
}

func tlsKCPListener(listener net.Listener, tlsConfig *tls.Config) net.Listener {
	if tlsConfig == nil {
		return listener
	}
	config := tlsConfig.Clone()
	config.NextProtos = []string{"http/1.1"}
	return tls.NewListener(listener, config)
}
