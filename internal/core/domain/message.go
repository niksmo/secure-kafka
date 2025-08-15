package domain

import "github.com/google/uuid"

type MessageID string

func newMessageID() MessageID {
	return MessageID(uuid.NewString())
}

func (m MessageID) String() string {
	return string(m)
}

type Message struct {
	ID   MessageID
	Data map[string]any
}

func NewMessage(data map[string]any) Message {
	return Message{
		ID:   newMessageID(),
		Data: data,
	}
}
