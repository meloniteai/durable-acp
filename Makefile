GOLANGCI_LINT_VERSION ?= v2.12.2
COVERAGE_MIN ?= 80

.PHONY: all test lint coverage

all: test lint coverage

test:
	go test -race ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

coverage:
	@coverage_file=$$(mktemp); \
	trap 'rm -f "$$coverage_file"' EXIT; \
	go test -covermode=atomic -coverprofile="$$coverage_file" ./...; \
	go tool cover -func="$$coverage_file"; \
	total=$$(go tool cover -func="$$coverage_file" | awk '/^total:/ { gsub(/%/, "", $$3); print $$3 }'); \
	awk -v total="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN { if (total + 0 < minimum + 0) { printf "coverage %.1f%% is below %.1f%%\n", total, minimum; exit 1 } }'
