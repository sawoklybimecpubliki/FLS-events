package main

import (
	"context"
	"fmt"
	"github.com/sawoklybimecpubliki/FLS-events/events"
	"log"
	"sync"
	"time"
)

var count map[string]int

type Respond struct {
	URL   string
	Count int
}

func main() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	if err := run(context.Background(), &wg); err != nil {
		log.Println("exit error: ", err)
	}
	wg.Wait()
	log.Println(ViewCount())
}

func run(ctx context.Context, wg *sync.WaitGroup) error {
	app := events.Service{
		BrokerAddr: "kafka:9092",
		KafkaConn:  events.NewConnection("kafka:9092"),
	}
	count = make(map[string]int)

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
		count[u]++
	}
}

func ViewCount() []Respond {
	var out []Respond
	for url, n := range count {
		out = append(out, Respond{URL: url, Count: n})
	}
	return out
}
