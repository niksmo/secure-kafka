package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/niksmo/secure-kafka/internal/adapter"
	"github.com/niksmo/secure-kafka/internal/core/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	config := loadConfig()

	stopCtx, stop := notifyContext()
	defer stop()

	tlsDialer := adapter.CreateTLSDialer(
		config.CACertPath, config.CertPath, config.KeyPath)

	kafkaProducer := adapter.NewKafkaProducer(
		logger,
		config.SeedBrokers,
		config.Topics,
		tlsDialer,
		config.ProducerLogin,
		config.ProducerPassword,
	)

	kafkaProducer.InitTopics(
		stopCtx, config.Partitions, config.ReplicationFactor)

	service := service.New(logger, kafkaProducer)

	mux := http.NewServeMux()
	adapter.RegisterHTTPHandler(logger, mux, service)

	handler := adapter.AcceptJSON(mux)

	server := &http.Server{
		Addr:    config.Addr,
		Handler: handler,
	}

	kafkaConsumer := adapter.NewKafkaConsumer(
		logger,
		config.SeedBrokers,
		config.Topics,
		config.ConsumerGroup,
		tlsDialer,
		config.ConsumerLogin,
		config.ConsumerPassword,
	)

	go runServer(logger, stop, server)
	go kafkaConsumer.Run(stopCtx)

	<-stopCtx.Done()
	closeServer(logger, server)
	kafkaProducer.Close()
	kafkaConsumer.Close()
}

type config struct {
	Addr              string
	SeedBrokers       []string
	Topics            []string
	Partitions        int
	ReplicationFactor int
	ConsumerGroup     string
	CACertPath        string
	CertPath          string
	KeyPath           string
	ProducerLogin     string
	ProducerPassword  string
	ConsumerLogin     string
	ConsumerPassword  string
}

func loadConfig() config {
	ca, cert, key := parseFlags()
	return config{
		Addr: ":8000",
		SeedBrokers: []string{
			"127.0.0.1:19094", "127.0.0.1:29094", "127.0.0.1:39094",
		},
		Topics:            []string{"topic_1", "topic_2"},
		Partitions:        1,
		ReplicationFactor: 1,
		ConsumerGroup:     "group_1",
		CACertPath:        ca,
		CertPath:          cert,
		KeyPath:           key,
		ProducerLogin:     "producer",
		ProducerPassword:  "pwd_producer",
		ConsumerLogin:     "consumer",
		ConsumerPassword:  "pwd_consumer",
	}
}

func parseFlags() (ca, cert, key string) {
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	execDir := filepath.Dir(execPath)
	certsPath := filepath.Join(execDir, "certs")

	flag.StringVar(&ca, "ca", filepath.Join(certsPath, "ca.crt"), "CA cert path")
	flag.StringVar(&cert, "cert", filepath.Join(certsPath, "client.crt"), "client cert path")
	flag.StringVar(&key, "key", filepath.Join(certsPath, "client.key"), "client private key path")

	flag.Parse()
	return
}

func notifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM,
	)
}

func runServer(
	log *slog.Logger, stop context.CancelFunc, server *http.Server,
) {
	log.Info("run server", "addr", server.Addr)

	err := server.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.Warn("server is crashed", "err", err.Error())
		stop()
	}
}

func closeServer(log *slog.Logger, server *http.Server) {
	timeoutCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	log.Info("start closing server...")
	err := server.Shutdown(timeoutCtx)
	if err != nil {
		log.Warn("failed to close server gracefully")
		return
	}
	log.Info("server closed successfully")
}
