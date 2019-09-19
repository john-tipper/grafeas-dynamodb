FROM golang:1.12.5
RUN apt-get update && apt-get install unzip
COPY . /go/src/github.com/john-tipper/grafeas-dynamodb/
WORKDIR /go/src/github.com/john-tipper/grafeas-dynamodb
RUN make build
WORKDIR /go/src/github.com/john-tipper/grafeas-dynamodb/go/v1beta1/main
RUN GO111MODULE=on CGO_ENABLED=0 go build -o grafeas-server .

FROM alpine:latest
WORKDIR /
COPY --from=0 /go/src/github.com/john-tipper/grafeas-dynamodb/go/v1beta1/main/grafeas-server /grafeas-server
EXPOSE 8080
ENTRYPOINT ["/grafeas-server"]
