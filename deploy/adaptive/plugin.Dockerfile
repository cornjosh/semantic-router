# Build context is plugins/higress-adaptive.
FROM golang:1.27.3 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN GOOS=wasip1 GOARCH=wasm go build -trimpath -buildmode=c-shared -o /plugin.wasm .
FROM scratch
COPY --from=build /plugin.wasm /plugin.wasm
