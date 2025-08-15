package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/niksmo/secure-kafka/internal/core/domain"
	"github.com/niksmo/secure-kafka/internal/core/port"
)

var _ port.MessageRegistrar = (*Service)(nil)

type Service struct {
	log      *slog.Logger
	producer port.MessageProducer
}

func New(log *slog.Logger, p port.MessageProducer) Service {
	return Service{log, p}
}

func (s Service) RegisterMessage(
	ctx context.Context, msg domain.Message,
) (domain.MessageID, error) {
	const op = "Service.RegisterMessage"

	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	err = s.producer.ProduceMessage(ctx, data)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return msg.ID, nil
}
