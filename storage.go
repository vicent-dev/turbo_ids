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

const MAX_BATCH_SIZE = 100_000

type storage struct {
	client         *mongo.Client
	db, collection string
	baseCriteria   bson.D
	rowsCount      map[string]int
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

func (s *storage) extractChunk(ctx context.Context, chunkSize int, wg *sync.WaitGroup, nThread int, fm *filesManager) error {
	defer wg.Done()

	start := chunkSize * nThread

	lastRecordInChunk := start + chunkSize

	alog.Info("Exporting records from %d to %d.", start, lastRecordInChunk)
	chunkRowsCount := 0

	var lsb strings.Builder

	collection := s.client.Database(s.db).Collection(s.collection)

	size := int(float64(chunkSize) * 0.1) // 10% chunkSize = batch size

	if size <= 0 {
		size = chunkSize
	}

	size = min(size, MAX_BATCH_SIZE)

	chunkKey := strconv.Itoa(start) + ":" + strconv.Itoa(lastRecordInChunk)

	for size != 0 {
		if os.Getenv("DEBUG_CHUNKS") == "true" {
			alog.Info("chunk[%s] start: %d end: %d", chunkKey, start, size)
		}

		cur, err := collection.Find(
			ctx,
			s.baseCriteria,
			options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetSkip(int64(start)).SetLimit(int64(size)),
		)

		if err != nil {
			alog.Error(err.Error())
			return err
		}

		for cur.Next(ctx) {
			var result map[string]any

			if err := cur.Decode(&result); err != nil {
				log.Fatal(err)
			}

			if ok, row := acceptAllRowsChecker(result); ok {
				chunkRowsCount += 1

				if row == "" {
					row = "\"" + (result["_id"]).(string) + "\","
				}

				lsb.WriteString(row + "\n")
			}
		}

		start = start + size

		if start+size > lastRecordInChunk {
			size = lastRecordInChunk - start
		}

		fm.writeInPartFile(lsb.String(), nThread)
		lsb.Reset()

		cur.Close(ctx)
	}

	s.rowsCount[chunkKey] = chunkRowsCount

	return nil
}
