package adapter

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func CreateTLSDialer(CACertPath, certPath, keyPath string) *tls.Dialer {
	const op = "CreateTLSDialer"

	caCert, err := os.ReadFile(CACertPath)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err))
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		panic(fmt.Errorf("%s: %s", op, "failed to parse CA cert"))
	}

	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err))
	}

	config := &tls.Config{
		RootCAs:      caCertPool,
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{clientCert},
	}

	return &tls.Dialer{
		Config: config,
	}
}
