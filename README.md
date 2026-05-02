# Nedbank DE Challenge — Starter Kit

This directory is your starting point for the Data Engineering track.

It contains a minimal scaffold that handles the Docker interface without
implementing any pipeline logic. Your task is to fill in the three pipeline
modules and make the Gold layer output pass the validation queries.

---

## Quick Start

### 1. Pull the base image

```bash
docker pull nedbank-de-challenge/base:1.0
```

If you cannot pull it, build it locally from the provided `Dockerfile.base`:

```bash
docker build -t nedbank-de-challenge/base:1.0 -f ../Dockerfile.base ..
```

### 2. Place sample data

Copy the sample data files (provided in `sample_data/` in the challenge pack)
to a local directory. The examples below use `/tmp/test-data`:

```bash
mkdir -p /tmp/test-data/input /tmp/test-data/config
cp ../sample_data/accounts_sample.csv     /tmp/test-data/input/accounts.csv
cp ../sample_data/transactions_sample.jsonl /tmp/test-data/input/transactions.jsonl
cp ../sample_data/customers_sample.csv    /tmp/test-data/input/customers.csv
cp config/pipeline_config.yaml            /tmp/test-data/config/
```

### 3. Implement the pipeline

Open and implement the three pipeline modules:

| File | Layer | What to build |
|---|---|---|
| `pipeline/ingest.py` | Bronze | Read raw source files → Delta tables |
| `pipeline/transform.py` | Silver | Deduplicate, type-cast, DQ-flag → Delta tables |
| `pipeline/provision.py` | Gold | Join, aggregate, surrogate keys → scored output |

Stage 3 only: also implement `pipeline/stream_ingest.py`.

### 4. Build your image

```bash
docker build -t my-submission:test .
```

### 5. Run locally with scoring-equivalent constraints

```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test

echo "Exit code: $?"
```

Your pipeline must exit with code 0 for the scoring system to read your output.

### 6. Verify your output

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
```

### 7. Run the local testing harness

From the challenge pack root (the directory containing `run_tests.sh`):

```bash
bash run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

All 7 checks must pass before you submit.

---

## Repository Layout

```
your-submission/
├── Dockerfile                      # Extends nedbank-de-challenge/base:1.0
├── requirements.txt                # Your extra Python dependencies (may be empty)
├── pipeline/
│   ├── __init__.py
│   ├── run_all.py                  # Entry point — do not rename
│   ├── ingest.py                   # Bronze layer — implement this
│   ├── transform.py                # Silver layer — implement this
│   ├── provision.py                # Gold layer — implement this
│   └── stream_ingest.py            # Stage 3 only — implement at Stage 3
├── config/
│   ├── pipeline_config.yaml        # Paths and Spark settings
│   └── dq_rules.yaml               # DQ rules (required from Stage 2)
├── stream/                             # Stage 3 stream data for local testing
│   ├── .gitkeep
│   └── README.md
├── adr/
│   └── stage3_adr.md               # Architecture Decision Record (Stage 3 only)
└── README.md                       # This file
```

**Important:** Do not commit the `output/` directory. It is already in `.gitignore`.

The `stream/` directory is used for Stage 3 local testing. Place the 12 stream batch
files there when you reach Stage 3. The `.jsonl` data files are excluded via `.gitignore`
— only `.gitkeep` and the README should be committed.

---

## Submission Tags

| Stage | Tag | Deadline |
|---|---|---|
| Stage 1 | `stage1-submission` | End of Day 7 |
| Stage 2 | `stage2-submission` | End of Day 14 |
| Stage 3 | `stage3-submission` | End of Day 21 |

The same repository carries all three stages — each tag points at the
commit that represents that stage's submission state. Tag names must
match exactly; `stage1`, `Stage1-submission`, etc. will not be found.

```bash
git tag -a stage2-submission -m "Stage 2 submission"
git push origin stage2-submission
```

---

## Stage 2 Notes

Stage 2 triples the data volume, injects six categories of DQ issues,
and adds a `merchant_subcategory` field. The pipeline handles all of
this without structural rewrites — Stage 2 amounts to:

- Switching the transaction reader to `text` + `from_json` with an
  explicit STRING-typed schema (so currency variants like `710` and
  string-quoted amounts both parse cleanly), plus a regex on the raw
  line to capture `_amount_was_string` for TYPE_MISMATCH detection.
- Adding `amount_type_mismatch` to the dq_report issue list.
- Building `transaction_timestamp` from the *normalised* date so DMY
  and Unix-epoch source dates produce a valid TIMESTAMP.

DQ rules — including which `dq_flag` is set, which records are
quarantined, and which are cast/normalised in place — are declared
entirely in `config/dq_rules.yaml`. No pipeline code needs to change
to swap `QUARANTINED` for `NORMALISED` on a given issue type; only
the handling action string in the YAML.

Stage 2 output adds `/data/output/dq_report.json` to the existing
Stage 1 Gold-layer output. All three Stage 1 validation queries
continue to pass on Stage 2 data with DQ handling applied.

---

## Stage 3 Notes

Stage 3 adds a streaming path on top of the Stage 2 batch pipeline.
The batch pipeline runs first to completion and writes the same
Stage 2 outputs; the streaming pass then polls `/data/stream/` for
micro-batch JSONL files and maintains two new Gold tables under
`/data/output/stream_gold/`:

- `current_balances` — one row per `account_id`, balance walked
  forward from the batch `dim_accounts.current_balance` baseline by
  applying CREDIT/REVERSAL as additions and DEBIT/FEE as subtractions.
- `recent_transactions` — last 50 events per `account_id`, merged on
  `(account_id, transaction_id)` and pruned per cycle.

The streaming pass is wired into the existing entry point: when a
`streaming:` block is present in `pipeline_config.yaml` (and the
`/data/stream/` directory exists), `run_all.py` calls
`run_stream_ingestion` after the batch pass; otherwise it skips it.
The polling loop exits once no new files have appeared for
`quiesce_timeout_seconds` (default 60s), well before the 30-minute
container timeout.

The schema and date-parsing helpers used by streaming are imported
directly from `ingest.py` and `transform.py` — no duplication. See
[`adr/stage3_adr.md`](adr/stage3_adr.md) for the full design rationale.

---

## Key References

| Document | What it covers |
|---|---|
| `docker_interface_contract.md` | Mount points, invocation flags, exit codes |
| `output_schema_spec.md` | Exact field names, types, and derivation rules |
| `submission_guide.md` | Tagging protocol, common mistakes, verification steps |
| `resource_constraints.md` | Memory (2 GB), CPU (2 vCPU), network (none) |
| `validation_queries.sql` | Three SQL queries the scorer runs against your Gold layer |
| `data_dictionary.md` | Source field definitions and data types |

---

## Resource Limits (summary)

| Resource | Limit |
|---|---|
| RAM | 2 GB hard ceiling |
| CPU | 2 vCPU — use `local[2]` for Spark |
| Wall-clock time | 30 minutes |
| Network | None during execution |
| Writable paths | `/data/output/` and `/tmp` (512 MB) only |

See `resource_constraints.md` for practical guidance on working within these limits.

---

## Development Notes

This pipeline was developed with the assistance of [Claude](https://claude.ai) (Anthropic),
an AI coding assistant. Claude was used to help design the medallion architecture,
write and review pipeline code, debug Spark/Delta Lake configuration issues, and
implement the Stage 2 data quality handling and reporting logic.
