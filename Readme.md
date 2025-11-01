# Turbo ids

Multi thread tool to export mongodb data based on row checker.

![meme](docs/meme.jpg)


## Instructions:
- Modify base find if possible `storage::getCount` and `storage::extractData`
- Add new function in `rowCheckers.go`
- Modify `storage.go:78` were the rows are processed.
- Build and run


Fork from https://github.com/vicent-dev/exporter