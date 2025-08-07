package core

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

type Stat struct {
	Name   string
	Number int
}

type Store struct {
	c *mongo.Collection
}

func NewStore(collection *mongo.Collection) (*Store, error) {
	mod := mongo.IndexModel{
		Keys: bson.M{"name": 1},
		//Options: options.Index().SetUnique(true),
	}

	if _, err := collection.Indexes().CreateOne(context.TODO(), mod); err != nil {
		return nil, fmt.Errorf("could not create index: %w", err)
	}

	return &Store{
		c: collection,
	}, nil
}

func (db *Store) InsertStat(ctx context.Context, s Stat) error {
	log.Println("stat: ", s)
	_, err := db.c.InsertOne(ctx, s)
	if err != nil {
		log.Println("error insert", err)
	}
	return err
}

func (db *Store) UpdateStat(ctx context.Context, s Stat) error {
	log.Println("stat: ", s)
	opts := options.Update().SetUpsert(true)
	filter := bson.D{{"name", s.Name}}
	update := bson.D{{"$set", bson.D{{"number", s.Number}}}}
	_, err := db.c.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Println("error insert", err)
	}
	return err
}

func (db *Store) GetStat(ctx context.Context, s Stat) (Stat, error) {
	existingStat := Stat{}

	err := db.c.FindOne(ctx, bson.M{"name": s.Name}).Decode(&existingStat)
	if err != nil {
		return Stat{}, fmt.Errorf("could not find stat: %w", err)
	}

	return existingStat, nil
}

func (db *Store) DeleteStat(ctx context.Context, s Stat) error {
	filter := bson.D{{
		Key:   "name",
		Value: s.Name,
	}}
	result, err := db.c.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	if result.DeletedCount == 0 {
		return nil
	}
	return nil
}
