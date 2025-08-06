package events

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"time"
)

type Event struct {
	URL string `json:"url"`
}

type Respond struct {
	URL   string
	Count int
}

type Service struct {
	BrokerAddr string
	KafkaConn  *kafka.Conn
}

var Count map[string]int

const topicName = "event"
const brokerAddr = "kafka:9092"

func NewConnection(brokerAddr string) *kafka.Conn {
	var kafkaConn *kafka.Conn
	var err error
	for range 5 {

		kafkaConn, err = kafka.DialLeader(context.Background(), "tcp", brokerAddr, topicName, 0)
		if err == nil {
			break
		}

		log.Println("Error connection to kafka: ", brokerAddr, err)
		time.Sleep(3 * time.Second)
	}
	log.Println("Connections success")
	return kafkaConn
}

func (s *Service) Produce(event *Event) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error JSON: %w", err)
	}

	msg := kafka.Message{
		Value: eventJSON,
		Time:  time.Now(),
	}

	_, err = s.KafkaConn.WriteMessages(msg)
	if err != nil {
		return fmt.Errorf("error write in kafka: %w", err)
	}
	return nil
}

func (s *Service) ConsumeOne(ctx context.Context) string {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddr},
		GroupID:     "Counter",
		Topic:       topicName,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})

	var out string

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		log.Printf("Error reading message: %v", err)
		return ""
	}

	log.Printf("Message received from topic %s, partition %d, offset %d: %s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

	if err := r.Close(); err != nil {
		log.Println("failed to close reader:", err)
		return err.Error()
	}

	return out
}

func (s *Service) ConsumeAll(ctx context.Context) []string {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddr},
		GroupID:     "Counter",
		Topic:       topicName,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})

	var out []string
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			break
		}

		out = append(out, string(m.Value))
		log.Printf("Message received from topic %s, partition %d, offset %d: %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Value))

		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}
	log.Println("OUT ALL: ", out)
	return out
}

func (s *Service) EventsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.Produce(&Event{
			URL: r.URL.Path,
		}); err != nil {
			log.Println("error send: ", err)
		}

		next.ServeHTTP(w, r)
	}
}

func ViewCount() []Respond {
	var out []Respond
	for url, n := range Count {
		out = append(out, Respond{URL: url, Count: n})
	}
	log.Println("out-------| ", out)
	return out
}
