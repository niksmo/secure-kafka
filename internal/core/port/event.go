package port

import (
	"context"

	"github.com/niksmo/secure-kafka/internal/core/domain"
)

type MessageRegistrar interface {
	RegisterMessage(context.Context, domain.Message) (domain.MessageID, error)
}

type MessageProducer interface {
	ProduceMessage(context.Context, []byte) error
}
