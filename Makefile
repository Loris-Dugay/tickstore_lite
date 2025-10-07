SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
REQ ?= requirements.txt

# >>> À compléter : crée tes recettes ici <<<
# Cibles imposées : setup, ingest, bars, sql-queries, test, grade

.PHONY: setup ingest bars sql-queries test grade

setup:
	@# crée le venv s'il n'existe pas, puis installe les deps dedans
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	@$(PIP) install --upgrade pip
	@$(PIP) install -r "$(REQ)"

ingest:
	@echo "TODO: implémenter ingestion CSV -> Parquet"

bars:
	@echo "TODO: implémenter barres 1s (OHLC, volume, VWAP)"

sql-queries:
	@echo "TODO: exécuter 5 requêtes SQL et produire un rapport lisible"

test:
	@echo "TODO: lancer pytest"

grade:
	@echo "TODO: produire out/grade_report.json et out/grade_report.txt"
