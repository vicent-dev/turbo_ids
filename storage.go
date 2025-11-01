package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/en-vee/alog"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type storage struct {
	client         *mongo.Client
	db, collection string
	baseCriteria   bson.D
	rowsCount      map[string]int
	data           map[string]string
}

func newStorage() (*storage, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO_CONNECTION")))

	if client == nil || err != nil {
		return nil, errors.New("mongodb connection did not succeed")
	}

	s := &storage{
		client:       client,
		db:           os.Getenv("MONGO_DB"),
		collection:   os.Getenv("MONGO_COLLECTION"),
		rowsCount:    make(map[string]int),
		data:         make(map[string]string),
		baseCriteria: bson.D{},
	}

	return s, nil
}

func (s *storage) disconnect(ctx context.Context) {
	if err := s.client.Disconnect(ctx); err != nil {
		panic(err)
	}
}

func (s *storage) getCount(ctx context.Context) (int64, error) {
	collection := s.client.Database(s.db).Collection(s.collection)

	return collection.CountDocuments(ctx, s.baseCriteria)
}

func (s *storage) extractChunk(ctx context.Context, start, size int, wg *sync.WaitGroup) error {
	defer wg.Done()

	alog.Info("Exporting records from %d to %d.", start, start+size)
	chunkRowsCount := 0

	var lsb strings.Builder

	collection := s.client.Database(s.db).Collection(s.collection)

	cur, err := collection.Find(
		ctx,
		s.baseCriteria,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetSkip(int64(start)).SetLimit(int64(size)),
	)

	if err != nil {
		alog.Error(err.Error())
		return err
	}

	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result map[string]any

		if err := cur.Decode(&result); err != nil {
			log.Fatal(err)
		}

		if ok, row := simpleRowChecker(result); ok {
			chunkRowsCount += 1

			if row == "" {
				row = "\"" + (result["_id"]).(string) + "\","
			}

			lsb.WriteString(row + "\n")
		}
	}

	chunkKey := strconv.Itoa(start) + ":" + strconv.Itoa(start+size)
	s.rowsCount[chunkKey] = chunkRowsCount
	s.data[chunkKey] = lsb.String()

	return nil
}
