package main

import (
	"context"
	"flag"
	"runtime"
	"sync"
	"turbo_ids/pkg/env"
	"turbo_ids/pkg/file"
	"turbo_ids/pkg/storage"

	"github.com/en-vee/alog"
)

func run() error {
	// process flags and load env vars
	env.LoadEnv()

	directory := flag.String("d", ".", "Directory to save the exported file")
	filename := flag.String("f", "data", "Filename exported file")

	flag.Parse()

	nThreads := runtime.GOMAXPROCS(0) - 1

	s, err := storage.NewStorage(nThreads)

	if err != nil {
		return err
	}

	ctx := context.TODO()

	fm := file.NewFilesManager(*directory, *filename, nThreads)

	if err := processInBatches(ctx, s, fm, nThreads); err != nil {
		return err
	} else {
		alog.Info("Data was exported: %s", fm.MainFilePath, nThreads)
	}

	return nil
}

func processInBatches(ctx context.Context, s *storage.Storage, fm *file.FilesManager, nThreads int) error {
	wg := sync.WaitGroup{}

	c64, err := s.GetCount(ctx)

	c := int(c64)

	if c == 0 {
		alog.Warn("No rows found based on criteria")
		return nil
	}

	if err != nil {
		return err
	}

	linesPerChunk := c / nThreads

	// for small exports we don't need more than one goroutines
	if c < 100 {
		linesPerChunk = c
		wg.Add(1)
		go s.ExtractChunk(ctx, linesPerChunk, &wg, 0, fm)
	} else {
		for i := 0; i <= nThreads; i++ {
			wg.Add(1)
			go s.ExtractChunk(ctx, linesPerChunk, &wg, i, fm)
		}
	}

	wg.Wait()

	totalRows, _ := fm.MergePartFiles()
	fm.RemovePartFiles()

	alog.Info("Count records exported: %d", totalRows)

	return nil
}

func main() {
	if err := run(); err != nil {
		alog.Error(err.Error())
	}
}
