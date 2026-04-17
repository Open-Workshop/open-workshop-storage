# Development

## Prerequisites

- Python 3.10+
- `7z` available in `PATH`

## Setup

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp ow_config_sample.py ow_config.py
```

## Run Locally

```bash
granian --working-dir src --interface asgi --host 127.0.0.1 --port 8000 open_workshop_storage.app:app
```

API docs will be available at `http://127.0.0.1:8000/`.

## Quality Commands

```bash
make format
make lint
make type-check
```

Current toolchain:

- `black`
- `isort`
- `flake8`
- `mypy`

Behavior:

- `make format` applies `isort` and `black`
- `make lint` checks `isort`, `black`, and `flake8`
- `make type-check` runs `mypy`

## Tests

```bash
python -m unittest discover -s tests -v
```

## Notes About Runtime

Do not run the service with multiple workers.

The current implementation keeps active jobs and WebSocket subscribers in process memory. For development,
that means a single Granian worker is the correct and expected runtime mode.

## Useful Files

- `ow_config_sample.py` - configuration template
- `token_gen.py` - token helper
- `Makefile` - local quality commands
- `setup.cfg` - `flake8`, `isort`, and `mypy` settings
