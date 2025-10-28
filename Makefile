SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
REQ ?= requirements.txt
CLI_MODULE := src.cli.main
CLI := $(PY) -m $(CLI_MODULE)

opt_arg = $(if $(strip $(2)),$(1) $(2))

SYMBOLS ?= BTCUSD BCHUSD MANAUSD
SYMBOLS_ARGS := $(foreach symbol,$(SYMBOLS),--symbols $(symbol))

define add_symbols
$(foreach s,$(1),--symbols $(s))
endef

GENERATE_CONFIG ?=
GENERATE_DATE_START ?=
GENERATE_DATE_END ?=
GENERATE_OUTPUT ?=
GENERATE_MARKET ?=
GENERATE_SYMBOLS ?=

ifeq ($(origin GENERATE_SYMBOLS),command line)
    ifeq ($(strip $(GENERATE_SYMBOLS)),)
        GENERATE_SYMBOLS_ARGS := $(call add_symbols,$(SYMBOLS))
    else
        GENERATE_SYMBOLS_ARGS := $(call add_symbols,$(GENERATE_SYMBOLS))
    endif
else
    # Pas passé en ligne de commande → on ne met RIEN (la config décide)
    GENERATE_SYMBOLS_ARGS :=
endif

GENERATE_ARGS := \
    $(call opt_arg,--config,$(GENERATE_CONFIG)) \
    $(call opt_arg,--date_start,$(GENERATE_DATE_START)) \
    $(call opt_arg,--date_end,$(GENERATE_DATE_END)) \
    $(call opt_arg,--output,$(GENERATE_OUTPUT)) \
    $(call opt_arg,--market,$(GENERATE_MARKET)) \
    $(GENERATE_SYMBOLS_ARGS)

DOWNLOAD_CONFIG ?=
DOWNLOAD_INPUT ?=
DOWNLOAD_OUTPUT ?=
DOWNLOAD_WORKERS ?=
DOWNLOAD_CHUNK_SIZE ?=
DOWNLOAD_TIMEOUT ?=
DOWNLOAD_RETRIES ?=
DOWNLOAD_BACKOFF ?=
DOWNLOAD_ARGS := \
        $(call opt_arg,--config,$(DOWNLOAD_CONFIG)) \
        $(call opt_arg,--input,$(DOWNLOAD_INPUT)) \
        $(call opt_arg,--output,$(DOWNLOAD_OUTPUT)) \
        $(call opt_arg,--workers,$(DOWNLOAD_WORKERS)) \
        $(call opt_arg,--chunk-size,$(DOWNLOAD_CHUNK_SIZE)) \
        $(call opt_arg,--timeout,$(DOWNLOAD_TIMEOUT)) \
        $(call opt_arg,--retries,$(DOWNLOAD_RETRIES)) \
        $(call opt_arg,--backoff,$(DOWNLOAD_BACKOFF))

TP_CONFIG ?=
TP_INPUT ?=
TP_OUTPUT ?=
TP_ARGS := \
        $(call opt_arg,--config,$(TP_CONFIG)) \
        $(call opt_arg,--input,$(TP_INPUT)) \
        $(call opt_arg,--output,$(TP_OUTPUT))

TB_CONFIG ?=
TB_INPUT ?=
TB_OUTPUT ?=
TB_BAR ?=
TB_ARGS := \
        $(call opt_arg,--config,$(TB_CONFIG)) \
        $(call opt_arg,--input,$(TB_INPUT)) \
        $(call opt_arg,--output,$(TB_OUTPUT)) \
        $(call opt_arg,--bar,$(TB_BAR))

DC_CONFIG ?=
DC_INPUT ?=
DC_OUTPUT ?=
DC_BAR ?=
DC_ARGS := \
        $(call opt_arg,--config,$(DC_CONFIG)) \
        $(call opt_arg,--input,$(DC_INPUT)) \
        $(call opt_arg,--output,$(DC_OUTPUT)) \
        $(call opt_arg,--bar,$(DC_BAR))

CHECK_CONFIG ?=
CHECK_INPUT ?=
CHECK_CHECK ?=
CHECK_ARGS := \
        $(call opt_arg,--config,$(CHECK_CONFIG)) \
        $(call opt_arg,--input,$(CHECK_INPUT)) \
        $(call opt_arg,--check,$(CHECK_CHECK))

VIS_CONFIG ?=
VIS_INPUT ?=
VIS_OUTPUT ?=
VIS_MODE ?=
VIS_ARGS := \
        $(call opt_arg,--config,$(VIS_CONFIG)) \
        $(call opt_arg,--input,$(VIS_INPUT)) \
        $(call opt_arg,--output,$(VIS_OUTPUT)) \
        $(call opt_arg,--mode,$(VIS_MODE))

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

.PHONY: help setup generate download transformation-preprocess transformation-bars download-check check visualisation check-results clear clear-test clear-env clear-all

help:
	@echo "Usage: make <target> [VARIABLE=value]"
	@echo
	@echo "Available targets:"
	@echo "  setup                     Create a virtualenv and install dependencies"
	@echo "  generate                  Run the URL generation CLI command"
	@echo "  download                  Run the downloader CLI command"
	@echo "  transformation-preprocess Run the preprocessing CLI command"
	@echo "  transformation-bars       Run the bars transformation CLI command"
	@echo "  visualisation             Run the visualisation CLI command"
	@echo "  download-check            Run the download-check CLI command"
	@echo "  check                     Run the checks CLI command"
	@echo "  check-results             Execute the full integration flow used in tests"
	@echo "  clear / clear-test        Remove generated artifacts"
	@echo "  clear-env                 Remove the virtual environment"
	@echo "  clear-all                 Remove artifacts, test data and virtualenv"

setup:
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	@$(PIP) install --upgrade pip
	@$(PIP) install -r "$(REQ)"

generate:
	$(CLI) generate $(strip $(GENERATE_ARGS))

download:
	$(CLI) download $(strip $(DOWNLOAD_ARGS))

transformation-preprocess:
	$(CLI) transformation-preprocess $(strip $(TP_ARGS))

transformation-bars:
	$(CLI) transformation-bars $(strip $(TB_ARGS))

download-check:
	$(CLI) download-check $(strip $(DC_ARGS))

check:
	$(CLI) check $(strip $(CHECK_ARGS))

visualisation:
	$(CLI) visualisation $(strip $(VIS_ARGS))

check-results:
	$(CLI) generate $(SYMBOLS_ARGS) --output $(OUTPUT_GENERATE_TEST)
	$(CLI) download --input $(OUTPUT_GENERATE_TEST) --output $(OUTPUT_DOWNLOAD_TEST)
	$(CLI) transformation-preprocess --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_PREPROCESS_TEST)
	$(CLI) transformation-bars --input $(OUTPUT_PREPROCESS_TEST) --output $(OUTPUT_BARS_TEST)
	$(CLI) download-check --input $(OUTPUT_DOWNLOAD_TEST) --output $(OUTPUT_CHECKS_TEST) --bar $(BAR)
	$(CLI) check --input $(INPUT_BARS_TEST) --check $(INPUT_CHECK_TEST)

clear:
	rm -rf $(OUTPUT_GENERATE)
	rm -rf $(DATA)

clear-test:
	rm -f $(OUTPUT_GENERATE_TEST)
	rm -rf $(DATA_TEST)

clear-env:
	rm -rf $(VENV)

clear-all:
	$(MAKE) clear
	$(MAKE) clear-test
	$(MAKE) clear-env
