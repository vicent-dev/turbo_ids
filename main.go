package main

import (
	"context"
	"flag"
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

	f, err, path := createFile(*directory, *filename)
	if err != nil {
		return err
	}

	if err := writeFileByChunks(ctx, f, s); err != nil {
		return err
	} else {
		alog.Info("Data was exported: %s", path)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		alog.Error(err.Error())
	}
}
