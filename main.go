package main

import (
	"context"
	"flag"
	"runtime"
	"sync"
	"time"

	"github.com/en-vee/alog"
)

func run() error {
	// process flags and load env vars
	loadEnv()

	directory := flag.String("d", ".", "Directory to save the exported file")
	filename := flag.String("f", "data", "Filename exported file")

	flag.Parse()

	s, err := newStorage()

	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()
	defer s.disconnect(ctx)

	nThreads := runtime.GOMAXPROCS(0)

	fm := newFilesManager(*directory, *filename, nThreads)

	if err := processInBatches(ctx, s, fm); err != nil {
		return err
	} else {
		alog.Info("Data was exported: %s", fm.mainFilePath)
	}

	return nil
}

func processInBatches(ctx context.Context, s *storage, fm *filesManager) error {
	wg := sync.WaitGroup{}

	c64, err := s.getCount(ctx)

	c := int(c64)

	if c == 0 {
		alog.Warn("No rows found based on criteria")
		return nil
	}

	if err != nil {
		return err
	}

	linesPerChunk := c / fm.nThreads

	// for small exports we don't need more than one goroutines
	if c < 100 {
		linesPerChunk = c
		wg.Add(1)
		go s.extractChunk(ctx, linesPerChunk, &wg, 0, fm)
	} else {
		for i := 0; i <= fm.nThreads; i++ {
			wg.Add(1)
			go s.extractChunk(ctx, linesPerChunk, &wg, i, fm)
		}
	}

	wg.Wait()

	fm.mergePartFiles()
	fm.removePartFiles()

	totalRows := 0

	for _, r := range s.rowsCount {
		totalRows += r
	}

	alog.Info("Count records exported: %d", totalRows)

	return nil
}

func main() {
	if err := run(); err != nil {
		alog.Error(err.Error())
	}
}
