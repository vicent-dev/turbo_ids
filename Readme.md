# Turbo ids

Multi thread tool to export mongodb data based on row checker.

![meme](docs/meme.jpg)

## Diagram flow:

![diagram](docs/diagram.png)

## Instructions:
- Add base criteria search in storage constructor (`pkg/storage/storage::NewStorage` function) if needed
- Add new function in `pkg/storage/rowCheckers.go`
- Modify `pkg/storage/storage.go:111` were the rows are processed.
- Build and run


Fork from https://github.com/vicent-dev/exporter