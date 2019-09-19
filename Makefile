## The lines below can be uncommented for debugging the make rules
#
# OLD_SHELL := $(SHELL)
# SHELL = $(warning Building $@$(if $<, (from $<))$(if $?, ($? newer)))$(OLD_SHELL)
#
# print-%:
# 	@echo $* = $($*)

.PHONY: build fmt test vet clean

SRC = $(shell find . -type f -name '*.go' -not -path "./protodeps/*")
CLEAN := *~

.EXPORT_ALL_VARIABLES:

GO111MODULE=on

build: vet fmt
	go build -v ./...

# http://golang.org/cmd/go/#hdr-Run_gofmt_on_package_sources
fmt:
	@gofmt -l -w $(SRC)

test:
	@go test -v ./...

vet:
	@go vet ./...

clean:
	go clean ./...
	rm -rf $(CLEAN)
