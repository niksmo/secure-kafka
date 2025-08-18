package adapter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"github.com/niksmo/secure-kafka/internal/core/port"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

var _ port.MessageProducer = (*KafkaProducer)(nil)

type KafkaProducer struct {
	log    *slog.Logger
	kcl    *kgo.Client
	topics []string
}

func NewKafkaProducer(
	log *slog.Logger,
	seedBrokers []string,
	topics []string,
	tlsDialer *tls.Dialer,
	saslUser, saslPass string,
) KafkaProducer {
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.Dialer(tlsDialer.DialContext),
		kgo.SASL(plain.Auth{
			User: saslUser,
			Pass: saslPass,
		}.AsMechanism()),
	)
	if err != nil {
		panic(err) // develop mistake
	}
	return KafkaProducer{log, kcl, topics}
}

func (p KafkaProducer) InitTopics(
	ctx context.Context, partitions int, replicationFactor int,
) {
	const op = "KafkaProducer.InitTopics"
	log := p.log.With("op", op)

	log.Info("start initializing topics...")
	cl := kadm.NewClient(p.kcl)
	_, err := cl.CreateTopics(
		ctx,
		int32(partitions),
		int16(replicationFactor),
		nil,
		p.topics...,
	)
	if err != nil {
		log.Error("failed to initialize topics", "err", err)
		return
	}

	log.Info("topics initialized")
}

func (p KafkaProducer) ProduceMessage(
	ctx context.Context, value []byte,
) error {
	const op = "KafkaProducer.ProduceMessage"

	if ctx.Err() != nil {
		return ctx.Err()
	}

	recs := p.createRecords(value)

	err := p.produce(ctx, recs)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (p KafkaProducer) Close() {
	const op = "KafkaProducer.Close"
	log := p.log.With("op", op)

	log.Info("start closing producer...")
	p.kcl.Close()
	log.Info("producer closed successfully")
}

func (p KafkaProducer) createRecords(value []byte) (recs []*kgo.Record) {
	for _, topic := range p.topics {
		recs = append(recs, &kgo.Record{
			Topic: topic,
			Value: value,
		})
	}
	return
}

func (p KafkaProducer) produce(ctx context.Context, recs []*kgo.Record) error {

	const op = "KafkaProducer.produce"
	log := p.log.With("op", op)

	var errs []error
	results := p.kcl.ProduceSync(ctx, recs...)
	for _, res := range results {
		err := res.Err
		if err != nil {
			topic := res.Record.Topic
			errs = append(errs, fmt.Errorf("%s: %w", topic, err))
			continue
		}
		log.Info("message produced",
			"topic", res.Record.Topic,
			"partition", res.Record.Partition)
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}
