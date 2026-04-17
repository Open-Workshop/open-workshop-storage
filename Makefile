.PHONY: type-check format lint

PYTHON ?= python3
SRC_DIRS := src tests

type-check:
	MYPYPATH=src $(PYTHON) -m mypy $(SRC_DIRS)

format:
	$(PYTHON) -m isort $(SRC_DIRS)

lint:
	$(PYTHON) -m flake8 $(SRC_DIRS)
