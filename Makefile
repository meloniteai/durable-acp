GOLANGCI_LINT_VERSION ?= v2.12.2
COVERAGE_MIN ?= 85
TEST_RESULTS ?=

.PHONY: all test lint vet coverage

all: lint vet test

test:
	@raw_coverage=$$(mktemp); \
	coverage_file=$$(mktemp); \
	trap 'rm -f "$$raw_coverage" "$$coverage_file"' EXIT; \
	if [ -n "$(TEST_RESULTS)" ]; then \
		mkdir -p "$$(dirname "$(TEST_RESULTS)")"; \
		if go test -json -race -covermode=atomic -coverprofile="$$raw_coverage" ./... > "$(TEST_RESULTS)"; then test_status=0; else test_status=$$?; fi; \
	else \
		if go test -race -covermode=atomic -coverprofile="$$raw_coverage" ./...; then test_status=0; else test_status=$$?; fi; \
	fi; \
	if [ "$$test_status" -ne 0 ]; then \
		if [ -n "$(TEST_RESULTS)" ]; then cat "$(TEST_RESULTS)"; fi; \
		exit "$$test_status"; \
	fi; \
	awk 'NR == 1 || ($$1 !~ /schema_(constants|helpers|types)_gen\.go:/ && $$1 !~ /\/internal\/cmd\/acpgen\/.*\.go:/)' "$$raw_coverage" > "$$coverage_file"; \
	go tool cover -func="$$coverage_file"; \
	total=$$(go tool cover -func="$$coverage_file" | awk '/^total:/ { gsub(/%/, "", $$3); print $$3 }'); \
	awk -v total="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN { if (total + 0 < minimum + 0) { printf "coverage %.1f%% is below %.1f%%\n", total, minimum; exit 1 } }'

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

vet:
	go vet ./...

coverage: test
