package httputils

import (
	"crypto/rand"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

var gCerts map[string]tls.Certificate
var gCertLock sync.RWMutex
var gHosts map[string]string
var gHostLock sync.RWMutex

// ServeTLS starts a secure HTTP server based on the provided address and cert paths.
func ServeTLS(addr string, certPath string, keyPath string) (net.Listener, error) {
	cert, err := loadCert(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	tlsConfig := tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequireAnyClientCert,
		InsecureSkipVerify: true} // Remove this in production - for self-signed keys
	tlsConfig.Rand = rand.Reader
	// Setup SSL server
	server := http.Server{}
	server.Addr = addr
	listener, err := tls.Listen("tcp", server.Addr, &tlsConfig)
	if err != nil {
		return nil, err
	}
	// Start serving requests
	go server.Serve(listener)
	return listener, nil
}

// Serve starts a regular HTTP server based on the provided address.
func Serve(addr string) (net.Listener, error) {
	server := http.Server{}
	server.Addr = addr
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, err
	}
	go server.Serve(listener)
	return listener, nil
}

// GetClient returns a secure http.Client instance pointer based on cert paths.
func GetClient(clientCertPath string, clientKeyPath string) (*http.Client, error) {
	var tlsConfig tls.Config
	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := loadCert(clientCertPath, clientKeyPath)
		if err != nil {
			return &http.Client{}, err
		}
		tlsConfig = tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true} // Remove this in production - for self-signed keys
	} else {
		tlsConfig = tls.Config{}
	}
	// Create new client using tls config
	trans := http.Transport{}
	trans.TLSClientConfig = &tlsConfig
	trans.DisableKeepAlives = true
	client := &http.Client{}
	client.Transport = &trans
	return client, nil
}

// GetHostname uses an http.Request object to determine the name of the sending host.
// Falls back to the IP address.
func GetHostname(r *http.Request) string {
	gHostLock.Lock()
	defer gHostLock.Unlock()
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if gHosts == nil {
		gHosts = make(map[string]string)
	}
	memoizedName, memoized := gHosts[ip]
	if memoized {
		return memoizedName
	}
	hosts, err := net.LookupAddr(ip)
	if err != nil || len(hosts) < 1 {
		return ip
	}
	host := strings.TrimSuffix(hosts[0], ".")
	gHosts[ip] = host
	return host
}

func loadCert(certPath string, keyPath string) (tls.Certificate, error) {
	gCertLock.Lock()
	defer gCertLock.Unlock()
	certKey := getCertKey(certPath, keyPath)
	if gCerts == nil {
		gCerts = make(map[string]tls.Certificate)
	}
	cert, exists := gCerts[certKey]
	if !exists {
		// Load cert from disk
		_, certErr := os.Stat(certPath)
		_, keyErr := os.Stat(keyPath)
		if os.IsNotExist(certErr) {
			return tls.Certificate{}, certErr
		}
		if os.IsNotExist(keyErr) {
			return tls.Certificate{}, keyErr
		}
		var err error
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return tls.Certificate{}, err
		}
		gCerts[certKey] = cert
	}
	return cert, nil
}

func getCertKey(cert string, key string) string {
	return cert + ":" + key
}
