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
	wg.Add(1)
	if err := run(context.Background(), &wg); err != nil {
		log.Println("exit error: ", err)
	}
	wg.Wait()
	log.Println(events.ViewCount())
}

func run(ctx context.Context, wg *sync.WaitGroup) error {
	app := events.Service{
		BrokerAddr: "kafka:9092",
		KafkaConn:  events.NewConnection("kafka:9092"),
	}
	events.Count = make(map[string]int)

	go func() {
		tick := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Exit loop")
				wg.Done()
				return
			case <-tick.C:
				answer := app.ConsumeAll(context.Background())
				CountingUsers(answer)
			}
		}
	}()
	return nil
}

func CountingUsers(url []string) {
	for _, u := range url {
		events.Count[u]++
	}
}
