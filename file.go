package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/en-vee/alog"
)

func createFile(directory string, filename string) (*os.File, error, string) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err, ""
	}

	path := fmt.Sprintf("%s/%s_%d.csv",
		directory,
		filename,
		time.Now().Unix(),
	)
	f, err := os.Create(path)

	return f, err, path
}

func writeFileByChunks(ctx context.Context, f *os.File, s *storage) error {
	wg := sync.WaitGroup{}
	maxWg := runtime.GOMAXPROCS(0)
	c64, err := s.getCount(ctx)

	c := int(c64)

	if err != nil {
		return err
	}

	linesPerChunk := c / maxWg

	// for small exports we don't need more than one goroutines
	if c < 100 {
		linesPerChunk = c
	}

	for i := 0; i <= maxWg; i++ {
		wg.Add(1)
		go writeChunk(ctx, f, i*linesPerChunk, linesPerChunk, s, &wg)
	}

	wg.Wait()

	alog.Info("Count records exported: %d", s.rowsCount)

	return nil
}

func writeChunk(ctx context.Context, f *os.File, start, size int, s *storage, wg *sync.WaitGroup) {
	defer wg.Done()
	w := bufio.NewWriter(f)

	if sb, cr, err := s.extractData(ctx, start, size); err == nil {
		w.WriteString(sb.String())
		s.rowsCount += cr
	} else {
		alog.Error(err.Error())
		return
	}

	if err := w.Flush(); err != nil {
		alog.Error(err.Error())
		return
	}
}
