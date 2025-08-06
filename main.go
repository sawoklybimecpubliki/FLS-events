package main

import (
	"context"
	"fmt"
	"github.com/sawoklybimecpubliki/FLS-events/events"
	"log"
	"sync"
	"time"
)

func main() {
	wg := sync.WaitGroup{}
	wg.Add(2)
	if err := run(context.Background(), &wg); err != nil {
		log.Println("exit error: ", err)
	}
	wg.Wait()
	log.Println(events.GetStats())
}

func run(ctx context.Context, wg *sync.WaitGroup) error {
	service := events.Service{
		BrokerAddr: "kafka:9092",
		KafkaConn:  events.NewConnection("kafka:9092"),
	}

	events.Count = make(map[string]int)
	kafkaConsumerCtx, kafkaCancelCtx := context.WithCancel(context.Background())
	msgCh := service.GetMsgChannel(kafkaConsumerCtx, wg)

	go func() {
		tick := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exit loop")
				kafkaCancelCtx()
				wg.Done()
				return
			case msg := <-msgCh:
				CountUsers(msg)
				log.Println("TICK: ", msg)
			case <-tick.C:
				log.Println(events.Count)
			}
		}
	}()
	return nil
}

func CountUsers(url events.Event) {
	events.Count[url.URL]++
}
