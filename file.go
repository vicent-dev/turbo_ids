package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
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

	if c == 0 {
		alog.Warn("No rows found based on criteria")
		return nil
	}

	if err != nil {
		return err
	}

	linesPerChunk := c / maxWg

	// for small exports we don't need more than one goroutines
	if c < 100 {
		linesPerChunk = c
		wg.Add(1)
		go writeChunk(ctx, f, 0, linesPerChunk, s, &wg)
	} else {
		for i := 0; i <= maxWg; i++ {
			wg.Add(1)
			go writeChunk(ctx, f, i*linesPerChunk, linesPerChunk, s, &wg)
		}
	}

	wg.Wait()

	totalRows := 0

	for _, r := range s.rowsCount {
		totalRows += r
	}

	alog.Info("Count records exported: %d", totalRows)

	return nil
}

func writeChunk(ctx context.Context, f *os.File, start, size int, s *storage, wg *sync.WaitGroup) {
	defer wg.Done()

	sb, cr, err := s.extractData(ctx, start, size)

	if err != nil {
		alog.Error(err.Error())
		return
	}

	w := bufio.NewWriter(f)
	w.WriteString(sb.String())

	if err := w.Flush(); err != nil {
		alog.Error(err.Error())
		return
	}

	chunkKey := strconv.Itoa(start) + ":" + strconv.Itoa(start+size)
	s.rowsCount[chunkKey] = cr
}
