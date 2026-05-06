# Development

## Prerequisites

- Rust toolchain with `cargo`
- `7z` available in `PATH`

## Setup

```bash
cargo check
```

## Run Locally

```bash
OPEN_WORKSHOP_PORT=8000 cargo run --bin distributor
OPEN_WORKSHOP_PORT=8001 cargo run --bin loader
```

## Quality Commands

```bash
make fmt
make fmt-check
make lint
make check
make test
```

## Tests

```bash
cargo test
```

## Notes About Runtime

The loader keeps job state and websocket subscribers in process memory. For development, run a single
loader process. For production, run one loader instance per storage root and keep the manager / access
service configuration in sync with the deployed environment.

## Useful Files

- `docs/CONFIGURATION.md` - environment-variable reference
- `src/bin/token_gen.rs` - token helper
- `Makefile` - Rust build and quality commands
