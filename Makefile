SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
REQ ?= requirements.txt
CLI_MODULE := src.cli.main

SYMBOLS := BTCUSD BCHUSD MANAUSD
SYMBOLS_ARGS := $(foreach symbol,$(SYMBOLS),--symbols $(symbol))

OUTPUT_GENERATE := src/downloader/urls_list.json

OUTPUT_GENERATE_TEST := tests/urls_list.json
OUTPUT_DOWNLOAD_TEST := tests/data/raw
OUTPUT_PREPROCESS_TEST := tests/data/transformed
OUTPUT_BARS_TEST := tests/data/bars/
OUTPUT_CHECKS_TEST := tests/data/checks

INPUT_BARS_TEST := tests/data/bars/1_minute
INPUT_CHECK_TEST := tests/data/checks/1_minute

DATA := data/
DATA_TEST := tests/data

BAR := 1m

.PHONY: setup check-results clear clear-test clear-env clear-all

setup:
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	@$(PIP) install --upgrade pip
	@$(PIP) install -r "$(REQ)"

check-results:
	$(PY) -m $(CLI_MODULE) generate $(SYMBOLS_ARGS) --output $(OUTPUT_GENERATE_TEST)
	$(PY) -m $(CLI_MODULE) download --input $(OUTPUT_GENERATE_TEST) --output $(OUTPUT_DOWNLOAD_TEST)
	$(PY) -m $(CLI_MODULE) transformation-preprocess --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_PREPROCESS_TEST)
	$(PY) -m $(CLI_MODULE) transformation-bars --input $(OUTPUT_PREPROCESS_TEST) --output $(OUTPUT_BARS_TEST)
	$(PY) -m $(CLI_MODULE) download-check --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_CHECKS_TEST) --bar $(BAR)
	$(PY) -m $(CLI_MODULE) check --input $(INPUT_BARS_TEST) --check $(INPUT_CHECK_TEST)

clear:
	rm -rf $(OUTPUT_GENERATE)
	rm -rf $(DATA)

clear-test:
	rm $(OUTPUT_GENERATE_TEST)
	rm -rf $(DATA_TEST)

clear-env:
	rm -rf $(VENV)

clear-all:
	clear
	clear-test
	clear-env
