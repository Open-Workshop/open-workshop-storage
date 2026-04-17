.PHONY: type-check format lint

PYTHON ?= $(if $(wildcard .venv/bin/python),./.venv/bin/python,python3)
SRC_DIRS := src tests

type-check:
	$(PYTHON) -m mypy $(SRC_DIRS)

format:
	$(PYTHON) -m isort $(SRC_DIRS)

lint:
	$(PYTHON) -m flake8 $(SRC_DIRS)
