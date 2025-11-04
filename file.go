package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/en-vee/alog"
)

type filesManager struct {
	mainFile     *os.File
	mainFilePath string
	directory    string
	filename     string
	time         int64
	nThreads     int
}

func newFilesManager(directory string, filename string, nThreads int) *filesManager {
	fm := filesManager{
		directory: directory,
		filename:  filename,
		time:      time.Now().Unix(),
		nThreads:  nThreads,
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		panic(err.Error())
	}

	for i := 0; i <= nThreads; i++ {
		_, err := os.Create(
			fmt.Sprintf("%s/%s_%d_part_%d.csv",
				fm.directory,
				fm.filename,
				fm.time,
				i,
			),
		)

		if err != nil {
			panic(err.Error())
		}
	}

	return &fm
}

func (fm *filesManager) writeInPartFile(fileContent string, nThread int) {

	f, err := os.OpenFile(
		fmt.Sprintf("%s/%s_%d_part_%d.csv",
			fm.directory,
			fm.filename,
			fm.time,
			nThread,
		),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		panic(err.Error())
	}

	defer f.Close()
	f.WriteString(fileContent)
}

func (fm *filesManager) mergePartFiles() (int, error) {
	var err error
	totalRows := 0

	path := fmt.Sprintf("%s/%s_%d.csv",
		fm.directory,
		fm.filename,
		fm.time,
	)

	fm.mainFilePath = path
	fm.mainFile, err = os.Create(path)
	defer fm.mainFile.Close()

	if err != nil {
		panic(err)
	}

	for i := 0; i <= fm.nThreads; i++ {
		pf, err := os.Open(
			fmt.Sprintf("%s/%s_%d_part_%d.csv",
				fm.directory,
				fm.filename,
				fm.time,
				i,
			),
		)

		if err != nil {
			panic(err)
		}

		scanner := bufio.NewScanner(pf)
		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			totalRows++

			fm.mainFile.WriteString(scanner.Text() + "\n")
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		pf.Close()

	}

	return totalRows, nil
}

func (fm *filesManager) removePartFiles() {
	for i := 0; i <= fm.nThreads; i++ {
		err := os.Remove(
			fmt.Sprintf("%s/%s_%d_part_%d.csv",
				fm.directory,
				fm.filename,
				fm.time,
				i,
			),
		)

		if err != nil {
			alog.Error(err.Error())
		}
	}
}
