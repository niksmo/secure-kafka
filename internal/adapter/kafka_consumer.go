package adapter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type KafkaConsumer struct {
	log *slog.Logger
	kcl *kgo.Client
}

func NewKafkaConsumer(
	log *slog.Logger,
	seedBrokers []string,
	topics []string,
	group string,
	tlsDialer *tls.Dialer,
	saslUser, saslPass string,
) KafkaConsumer {
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.Dialer(tlsDialer.DialContext),
		kgo.SASL(plain.Auth{
			User: saslUser,
			Pass: saslPass,
		}.AsMechanism()),
	)
	if err != nil {
		panic(err) // develop mistake
	}
	return KafkaConsumer{log, kcl}
}

func (c KafkaConsumer) Run(ctx context.Context) {
	const op = "KafkaConsumer.Run"
	log := c.log.With("op", op)

	log.Info("run consumer")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.consume(ctx)
			if err != nil {
				log.Error("%s: %w", op, err)
			}
		}
	}
}

func (c KafkaConsumer) Close() {
	const op = "KafkaConsumer.Close"
	log := c.log.With("op", op)

	log.Info("start closing consumer...")
	c.kcl.Close()
	log.Info("consumer closed successfully")
}

func (c KafkaConsumer) consume(ctx context.Context) error {
	const op = "KafkaConsumer.consume"

	fetches, err := c.getFetches(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	c.handleTopics(fetches)

	err = c.commit(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (c KafkaConsumer) getFetches(ctx context.Context) (kgo.Fetches, error) {
	const op = "KafkaConsumer.getFetches"
	log := c.log.With("op", op)

	var errs []error

	fetches := c.kcl.PollFetches(ctx)
	if errors.Is(fetches.Err0(), context.Canceled) {
		return nil, nil
	}

	fetches.EachError(func(topic string, _ int32, err error) {
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			log.Warn("authorization failed", "topic", topic)
			return
		}
		errs = append(errs, fmt.Errorf("topic: %w", err))
	})

	if len(errs) != 0 {
		return nil, fmt.Errorf("%s: %w", op, errors.Join(errs...))
	}

	return fetches, nil
}

func (c KafkaConsumer) handleTopics(fetches kgo.Fetches) {
	const op = "KafkaConsumer.handleTopics"
	log := c.log.With("op", op)

	fetches.EachTopic(func(ft kgo.FetchTopic) {
		ft.EachRecord(func(r *kgo.Record) {
			log.Info(
				"read message",
				"topic", ft.Topic,
				"message", string(r.Value),
			)
		})
	})
}

func (c KafkaConsumer) commit(ctx context.Context) error {
	const op = "KafkaConsumer.commit"

	err := c.kcl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}
