.PHONY: build check test fmt fmt-check lint run-distributor run-loader release

CARGO ?= cargo

build:
	$(CARGO) build

check:
	$(CARGO) check

test:
	$(CARGO) test

fmt:
	$(CARGO) fmt

fmt-check:
	$(CARGO) fmt --check

lint:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

run-distributor:
	$(CARGO) run --bin distributor

run-loader:
	$(CARGO) run --bin loader

release:
	$(CARGO) build --release
