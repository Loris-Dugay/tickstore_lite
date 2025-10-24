SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
REQ ?= requirements.txt
CLI_MODULE := src.cli.main

SYMBOLS := BTCUSD BCHUSD MANAUSD
OUTPUT_GENERATE := tests/urls_list.json
SYMBOLS_ARGS := $(foreach symbol,$(SYMBOLS),--symbols $(symbol))

OUTPUT_DOWNLOAD_TEST := tests/data/raw
OUTPUT_PREPROCESS_TEST := tests/data/transformed
OUTPUT_BARS_TEST := tests/data/bars/
OUTPUT_CHECKS_TEST := tests/data/checks

INPUT_BARS_TEST := tests/data/bars/1_minute
INPUT_CHECK_TEST := tests/data/checks/1_minute

BAR := 1m

.PHONY: setup check-results

setup:
	@# crée le venv s'il n'existe pas, puis installe les deps dedans
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	@$(PIP) install --upgrade pip
	@$(PIP) install -r "$(REQ)"

check-results:
	$(PY) -m $(CLI_MODULE) generate $(SYMBOLS_ARGS) --output $(OUTPUT_GENERATE)
	$(PY) -m $(CLI_MODULE) download --input $(OUTPUT_GENERATE) --output $(OUTPUT_DOWNLOAD_TEST)
	$(PY) -m $(CLI_MODULE) transformation-preprocess --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_PREPROCESS_TEST)
	$(PY) -m $(CLI_MODULE) transformation-bars --input $(OUTPUT_PREPROCESS_TEST) --output $(OUTPUT_BARS_TEST)
	$(PY) -m $(CLI_MODULE) download-check --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_CHECKS_TEST) --bar $(BAR)
	$(PY) -m $(CLI_MODULE) check --input $(INPUT_BARS_TEST) --check $(INPUT_CHECK_TEST)
