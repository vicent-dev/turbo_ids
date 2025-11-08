run:
	go run cmd/cli/main.go -f=dump -d=data
build:
	go build cmd/cli/main.go
test:
	go test turbo_ids -v
