SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
REQ ?= requirements.txt
CLI_MODULE := src.cli.main

SYMBOLS := BTCUSD BCHUSD MANAUSD
OUTPUT_GENERATE := tests/urls_list.json
SYMBOLS_ARGS := $(foreach symbol,$(SYMBOLS),--symbols $(symbol))

OUTPUT_DOWNLOAD := tests/data/raw
OUTPUT_PREPROCESS := tests/data/transformed
OUTPUT_BARS := tests/data/bars/
OUTPUT_CHECKS := tests/data/checks

INPUT_BARS := tests/data/bars/1_minute
INPUT_CHECK := tests/data/checks/1_minute

BAR := 1m

.PHONY: setup check-results

setup:
	@# crée le venv s'il n'existe pas, puis installe les deps dedans
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	@$(PIP) install --upgrade pip
	@$(PIP) install -r "$(REQ)"

check-results:
	$(PY) -m $(CLI_MODULE) generate $(SYMBOLS_ARGS) --output $(OUTPUT_GENERATE)
	$(PY) -m $(CLI_MODULE) download --input $(OUTPUT_GENERATE) --output $(OUTPUT_DOWNLOAD)
	$(PY) -m $(CLI_MODULE) transformation-preprocess --input $(OUTPUT_DOWNLOAD) --output $(OUTPUT_PREPROCESS)
	$(PY) -m $(CLI_MODULE) transformation-bars --input $(OUTPUT_PREPROCESS) --output $(OUTPUT_BARS)
	$(PY) -m $(CLI_MODULE) download-check --input $(OUTPUT_DOWNLOAD) --output $(OUTPUT_CHECKS) --bar $(BAR)
	$(PY) -m $(CLI_MODULE) check --input $(INPUT_BARS) --check $(INPUT_CHECK)
