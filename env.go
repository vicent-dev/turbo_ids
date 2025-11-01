package main

import (
	"github.com/en-vee/alog"
	"github.com/joho/godotenv"
)

func loadEnv() {
	err := godotenv.Load(".env")

	if err != nil {
		alog.Warn("Loading env vars from system", err.Error())
	}
}
