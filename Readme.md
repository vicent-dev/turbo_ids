# Turbo ids

Multi thread tool to export mongodb data based on row checker.

![meme](docs/meme.jpg)

## Diagram flow:

![diagram](docs/diagram.png)

## Instructions:
- Configure env vars in .env file.
- Add base criteria search in storage constructor (`pkg/storage/storage::NewStorage`) if needed
- Create a new model  `pkg/storage/model.go` implementing Row interface:
    - `IsValid()` is the method where you have to put the row checker.
    - `String()` will contain the serialization of your model into the csv.

- Change the type of row object container in  `pkg/storage/storage.go:105`.
- Build and run


Fork from https://github.com/vicent-dev/exporter
