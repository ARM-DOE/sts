package util

import (
    "crypto/rand"
    "crypto/tls"
    "net"
    "net/http"
    "os"
    "time"
)

// cached_certs is a map of certs that have already been loaded by LoadCert().
// If a cert with the same cert_path and key_path is requested from LoadCert()
// it doesn't need to be loaded from disk again.
var cached_certs map[string]tls.Certificate

// AyncListenAndServeTLS spawns a new goroutine with a TLS server listening on the returned
// net.Listener. It returns the internal net.Listener object so that it can be closed when needed.
func AsyncListenAndServeTLS(addr string, certFile string, keyFile string) (net.Listener, error) {
    ssl_cert, cert_load_err := LoadCert(certFile, keyFile)
    if cert_load_err != nil {
        return nil, cert_load_err
    }
    ssl_config := tls.Config{
        Certificates:       []tls.Certificate{ssl_cert},
        ClientAuth:         tls.RequireAnyClientCert,
        InsecureSkipVerify: true} // Remove this in production - for self-signed keys
    ssl_config.Rand = rand.Reader
    // Setup SSL server
    server := http.Server{}
    server.Addr = addr
    ssl_listener, listener_err := tls.Listen("tcp", server.Addr, &ssl_config)
    if listener_err != nil {
        return nil, listener_err
    }
    // Start serving requests
    go server.Serve(ssl_listener)
    return ssl_listener, nil
}

// AsyncListenAndServe is the plain HTTP equivalent of AsyncListenAndServeTLS. It returns a net.Listener
// which can be closed at any time.
func AsyncListenAndServe(addr string) (net.Listener, error) {
    server := http.Server{}
    server.Addr = addr
    listener, listener_err := net.Listen("tcp", server.Addr)
    if listener_err != nil {
        return nil, listener_err
    }
    go server.Serve(listener)
    return listener, nil
}

// LoadCert checks the list of pre-loaded certs and returns one if it has already been
// loaded in the past. If it hasn't already been loaded, the cert and key file are read
// from disk, cached, and returned as a tls.Certificate.
func LoadCert(cert_path string, key_path string) (tls.Certificate, error) {
    cert_key := getCertKey(cert_path, key_path)
    var return_cert tls.Certificate
    if cached_certs == nil {
        cached_certs = make(map[string]tls.Certificate)
    }
    cached_cert, have_cert := cached_certs[cert_key]
    if have_cert {
        return_cert = cached_cert
    } else {
        // Load cert from disk
        _, cert_found := os.Stat(cert_path)
        _, key_found := os.Stat(key_path)
        if os.IsNotExist(cert_found) {
            return tls.Certificate{}, cert_found
        }
        if os.IsNotExist(key_found) {
            return tls.Certificate{}, key_found
        }
        var cert_err error
        return_cert, cert_err = tls.LoadX509KeyPair(cert_path, key_path)
        if cert_err != nil {
            return tls.Certificate{}, cert_err
        }
        cached_certs[cert_key] = return_cert
    }
    return return_cert, nil
}

// getCertKey returns a string used as a key for storing certs in cached_certs.
// The format of the string is "cert_path:key_path"
func getCertKey(cert string, key string) string {
    return cert + ":" + key
}

// GetTLSClient returns an http.Client with the given cert in its configuration.
func GetTLSClient(client_cert string, client_key string, load_certs bool) (http.Client, error) {
    var tls_config tls.Config
    if load_certs {
        cert, cert_err := LoadCert(client_cert, client_key)
        if cert_err != nil {
            return http.Client{}, cert_err
        }
        tls_config = tls.Config{
            Certificates:       []tls.Certificate{cert},
            InsecureSkipVerify: true} // Remove InsecureSkipVerify in production - for self-signed keys
    } else {
        tls_config = tls.Config{}
    }
    // Create new client using tls config
    client_transport := http.Transport{}
    client_transport.TLSClientConfig = &tls_config
    client_transport.DisableKeepAlives = true
    new_client := http.Client{}
    new_client.Timeout = time.Hour // Requests can't take longer than an hour
    new_client.Transport = &client_transport
    return new_client, nil
}
