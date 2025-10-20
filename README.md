# Project 1 — TickStore Lite (SQUELETTE) 🧱

Ce dépôt est **un squelette vide** pour réaliser le projet. Aucun code n’est fourni, seulement la structure,
les contraintes, le barème et les modalités de rendu. **À toi d’implémenter.**

---

## Objectif
Construire un mini **tick data lake** local :
- Ingestion de **trades** (et, optionnellement, **quotes**) depuis CSV.
- Écriture en **Parquet partitionné** (`date=YYYY-MM-DD/symbol=SYMB`).
- Calcul de **barres 1s** (OHLC, volume, **VWAP**).
- Exécution de **requêtes SQL** (DuckDB **ou** ClickHouse).
- (Bonus) Jobs **Spark** équivalents batch (et streaming si tu veux).

---

## À utiliser **absolument**
1. **Python 3.10+** et **SQL** (DuckDB **ou** ClickHouse — au moins l’un des deux).
2. **Parquet** avec **partitionnement** par **date** et **symbol**.
3. Une **CLI Python** (Typer ou argparse) exposant au minimum :
   - `ingest` : CSV → Parquet
   - `bars` : Parquet → barres 1s
   - `checks` : contrôles de base (non-vide, nulls critiques, etc.)
4. Un **Makefile** avec ces cibles (noms imposés) :
   - `make setup` (création venv + install deps)
   - `make ingest`
   - `make bars`
   - `make sql-queries` (exécute 5 requêtes types et écrit des résultats lisibles)
   - `make test` (pytest)
   - `make grade` (rapport JSON/TXT dans `out/` — tu définis la logique)
5. **Tests PyTest** (au moins 3), couvrant ingestion + barres.
6. **README clair** expliquant comment lancer chaque étape.
7. **Gestion de l’horodatage** : `ts` en UTC (ou traité comme UTC) **sans mélange de timezones**.

### Schémas **minimaux** attendus
- **Trades** : `ts` (ISO8601), `symbol` (str), `price` (float), `size` (int), `exchange` (str)
- **Quotes (optionnel)** : `ts`, `symbol`, `bid`, `bid_size`, `ask`, `ask_size`, `exchange`

---

## Interdit / Non comptabilisé
- ❌ **Sorties finales uniquement en CSV** (le Parquet partitionné est obligatoire).
- ❌ **Notebooks** comme **unique** pipeline (ok pour l’exploration, mais la CLI est requise).
- ❌ **C++/Rust** pour ce projet (on reste **Python/SQL** ; Spark en bonus).
- ❌ Dépendances cloud payantes ou services nécessitant des clés privées.
- ❌ Commits d’artéfacts lourds (>50 MB) dans le repo (données d’exemple légères ok).
- ❌ Modifier le barème ci-dessous dans ton rendu (tu peux proposer un barème *bonus* séparé).

---

## Contraintes supplémentaires
- **Idempotence** : relancer `make ingest`/`make bars` ne doit pas casser le lake (écritures déterministes).
- **Reproductibilité** : tout doit se lancer depuis zéro avec les cibles Make.
- **Performance indicative (local)** : le dataset d’exemple doit s’exécuter en < 10 s.

---

## Barème (100 pts)
- **Ingestion & partitionnement** (20 pts)
- **Barres 1s exactes (OHLC, volume, VWAP)** (30 pts)
- **Requêtes SQL (5 mini)** (10 pts)
- **Qualité & idempotence (checks, re-runs OK)** (15 pts)
- **Automatisation & clarté (Makefile/README/tests)** (15 pts)
- **(Bonus) Spark track** (10 pts)

> La note finale inclut une courte revue de code (lisibilité, structure, docstrings, logs).

---

## Modalités de rendu
**Option A — GitHub (préféré)**  
- Repo public nommé `tickstore-lite-<ton_pseudo>`
- Inclure ce README (complété), le **Makefile** et les scripts.
- Tag `v1.0` quand c’est prêt.

**Option B — Archive**  
- Envoyer un `.zip` du dossier (sans venv), avec `data/sample/` **léger** pour reproduire.

### Ce que je lancerai pour te noter
```bash
make setup
make ingest
make bars
make sql-queries
make test
make grade
```
- Les résultats attendus :  
  - Parquet sous `data/lake/` et `data/derived/bars_1s/` (partitionné `date, symbol`).
  - `out/grade_report.json` + `out/grade_report.txt` avec métriques clés (lignes ingérées, nb de barres, timings, checks OK/KO).

---

## Structure fournie (à compléter)
```
.
├── README.md
├── requirements.txt               # à compléter par toi
├── requirements-spark.txt         # optionnel (Spark)
├── pyproject.toml                 # metadata outillage (peut rester minimal)
├── Makefile                       # cibles imposées (TODO)
├── conf/
│   └── example_config.yaml        # exemple de config (TODO)
├── src/
│   └── tickstore/
│       ├── __init__.py
│       ├── cli.py                 # CLI (vide)
│       ├── ingest.py              # ingestion (vide)
│       ├── compute_bars.py        # barres 1s (vide)
│       └── quality_checks.py      # checks (vide)
├── jobs/
│   └── spark/
│       ├── batch_ingest.py        # (vide)
│       └── compute_bars_spark.py  # (vide)
├── tests/
│   └── test_sample.py             # (vide) — à remplir
├── tools/
│   └── grade.py                   # (vide) — à définir par toi
├── data/
│   ├── sample/                    # mets quelques CSV légers ici
│   ├── lake/                      # sorties Parquet (ingestion)
│   └── derived/
│       └── bars_1s/               # sorties Parquet (barres)
└── .github/workflows/ci.yml       # pytest basique (TODO)
```

---

## Conseils
- Commence par un petit dataset (2–3 symboles, quelques secondes).  
- Valide d’abord en **DuckDB** (plus simple), puis ajoute **ClickHouse** si tu veux.  
- Garde les **logs structurés** (niveau INFO), et un `--dry-run` utile.  
- La piste **Spark** est un **bonus** (montre batch d’abord).

Bon courage ! 🚀
