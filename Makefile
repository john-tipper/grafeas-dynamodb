## The lines below can be uncommented for debugging the make rules
#
# OLD_SHELL := $(SHELL)
# SHELL = $(warning Building $@$(if $<, (from $<))$(if $?, ($? newer)))$(OLD_SHELL)
#
# print-%:
# 	@echo $* = $($*)

.PHONY: build fmt test vet clean generate

SRC = $(shell find . -type f -name '*.go' -not -path "./grafeas/grafeas/*")
CLEAN := *~

.EXPORT_ALL_VARIABLES:

GO111MODULE=on

build: vet fmt generate
	go build -v ./...

# http://golang.org/cmd/go/#hdr-Run_gofmt_on_package_sources
fmt:
	@gofmt -l -w $(SRC)

pre-test:
	mkdir -p test
	bash -c "if [ ! -f test/dynamodb_local_latest.tgz ]; then curl https://s3.eu-central-1.amazonaws.com/dynamodb-local-frankfurt/dynamodb_local_latest.tar.gz -o test/dynamodb_local_latest.tgz -L; fi"
	tar xf test/dynamodb_local_latest.tgz -C test
	bash -c 'cd test; java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb & echo $$! > dynamo.pid'

test: generate
	@go test -v ./...

post-test:
	bash -c "kill $$(cat test/dynamo.pid)"
	rm -f test/dynamo.pid

vet: generate
	@go vet ./...

generate:
	mkdir -p grafeas
	bash -c "if [ ! -f grafeas/grafeas.tgz ]; then curl https://github.com/grafeas/grafeas/releases/download/v0.1.3/grafeas-0.1.3.tar.gz -o grafeas/grafeas.tgz -L; fi"
	tar xf grafeas/grafeas.tgz -C grafeas

clean: generate
	go clean ./...
	rm -rf $(CLEAN)
	rm -rf test grafeas
