.PHONY: build test install clean

BINARY_NAME = workflow-plugin-audit
INSTALL_DIR ?= data/plugins/$(BINARY_NAME)
INSTALL_PATH = $(if $(DESTDIR),$(DESTDIR)/$(INSTALL_DIR),$(INSTALL_DIR))
GO_ENV = GOWORK=off GOPRIVATE=github.com/GoCodeAlone/*
VERSION ?= $(shell jq -r .version plugin.json)
LDFLAGS = -X github.com/GoCodeAlone/workflow-plugin-audit/internal.Version=$(VERSION)

build:
	$(GO_ENV) go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) ./cmd/$(BINARY_NAME)

test:
	$(GO_ENV) go test ./... -v -race

install: build
	mkdir -p $(INSTALL_PATH)
	cp bin/$(BINARY_NAME) $(INSTALL_PATH)/
	cp plugin.json $(INSTALL_PATH)/
	cp plugin.contracts.json $(INSTALL_PATH)/

clean:
	rm -rf bin/
