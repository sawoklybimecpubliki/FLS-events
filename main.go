package main

import (
	"context"
	"fmt"
	"github.com/sawoklybimecpubliki/FLS-events/core"
	"github.com/sawoklybimecpubliki/FLS-events/events"
	"github.com/sawoklybimecpubliki/FLS-events/internal/foundation"
	"github.com/spf13/viper"
	"log"
	"strings"
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
}

func run(ctx context.Context, wg *sync.WaitGroup) error {

	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	mongoClient, err := foundation.NewMongoClient(ctx, foundation.MongoConfig{
		Host:     cfg.Mongo.Host,
		Port:     cfg.Mongo.Port,
		Username: cfg.Mongo.Username,
		Password: cfg.Mongo.Password,
	})
	if err != nil {
		return err
	}

	statStore, err := core.NewStore(mongoClient.Database(cfg.Mongo.DBName).Collection(cfg.Mongo.StatsCollection))
	if err != nil {
		return err
	}

	service := events.Service{
		BrokerAddr: "kafka:9092",
		KafkaConn:  events.NewConnection("kafka:9092"),
		StatStore:  statStore,
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
				if err := service.StatStore.InsertStat(ctx, core.Stat{
					Name:   msg.URL,
					Number: events.Count[msg.URL],
				}); err != nil {
					log.Println("error insert", err)
				}
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

type Config struct {
	App struct {
		ConnectionTimeoutSec int
	}

	Mongo struct {
		Host            string
		Port            string
		Username        string
		Password        string
		DBName          string
		StatsCollection string
	}
}

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./cmd/api")
	viper.AddConfigPath(".")

	viper.SetEnvPrefix("US")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("could not read config: %w", err)
	}

	cfg := &Config{}

	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	return cfg, nil
}
