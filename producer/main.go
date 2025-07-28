package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type Service struct {
	c *kafka.Conn
}

func NewConnect(ctx context.Context, topic string, partition int) *kafka.Conn {
	conn, err := kafka.DialLeader(ctx, "tcp", "localhost:29092", topic, partition)
	if err != nil {
		log.Println("failed to dial leader:", err)
	}
	return conn
}

func (s *Service) WriteMessage(mess string) error {
	s.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := s.c.WriteMessages(
		kafka.Message{Value: []byte(mess)},
	)
	if err != nil {
		log.Println("failed to write messages: ", err)
		return err
	}
	return nil
}

func ReadMessage() error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   "my-topic",
		GroupID: "my-groupID",
	})

	defer r.Close()

	for range 7 {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
		return err
	}
	return nil
}
