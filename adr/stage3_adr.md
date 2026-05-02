# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Jumar Vermeulen
**Date:** 2026-05-02
**Status:** Final

---

## Context

The Stage 3 brief required a near-real-time path to surface account
balances and recent transactions for the mobile app, alongside the
existing daily batch. The streaming interface itself is intentionally
minimal: a directory of 12 micro-batch JSONL files (50–500 events
each) is pre-staged at `/data/stream/` and the pipeline must poll the
directory, process new files in filename order, and reflect each
event in two new Gold tables — `current_balances` (one row per
account, upsert semantics) and `recent_transactions` (last 50 events
per account, merge semantics) — within a 5-minute SLA measured from
event timestamp to `updated_at`. The Stage 2 batch pipeline must
continue to run correctly: same `accounts.csv`, `customers.csv`, and
`transactions.jsonl` at `/data/input/`, same six DQ injections, same
`dq_report.json` and validation queries.

The pipeline entered Stage 3 at ~250 LOC across five Python modules:
`ingest.py` / `transform.py` / `provision.py` for the medallion
layers, `run_all.py` as a thin orchestrator that loads
`pipeline_config.yaml` + `dq_rules.yaml` and calls the three layers
in sequence, and `spark_utils.py` holding the SparkSession factory.
Stage 1 → Stage 2 was a +395 / −44 line surgical extension; the
intent going into Stage 3 was the same shape of diff again.

---

## Decision 1: How did the existing architecture facilitate or hinder the streaming extension?

**What helped.** Three Stage 1/2 choices made Stage 3 almost a
drop-in. First, the explicit `TRANSACTION_SCHEMA` in `ingest.py` —
introduced in Stage 2 to handle mixed-type DQ injections — was reused
verbatim in `stream_ingest.py` by a single
`from pipeline.ingest import TRANSACTION_SCHEMA`. Stream events share
the Stage 2 transaction shape so all the ingestion robustness
(numeric currencies, string-quoted amounts, epoch dates) carried
over for free. Second, `_parse_date_flex` in `transform.py` was
extracted as a private helper rather than inlined; `stream_ingest`
imports and calls it directly, no copy-paste. Third,
`pipeline_config.yaml` already had a commented-out `streaming:`
block from Stage 1, so wiring the new path was uncommenting that
block plus six lines in `run_all.py` guarded on
`config.get("streaming")`.

**What created friction.** The single `get_or_create_spark` factory
held assumptions tuned for one large batch shuffle:
`shuffle.partitions=32` is right-sized for 3M batch rows but
oversized for a 200-event micro-batch where the merge plan generates
~32 nearly empty tasks per file. AQE's `coalescePartitions` hides
most of this in practice but it is still wasted overhead. The
bigger friction was the Stage 1 decision to wire `run_all.py` as a
flat script with a single `if __name__ == "__main__"` body — fine
for batch, but the streaming loop had to be appended inline rather
than being a peer entry point.

**Code survival rate.** All five existing modules survived intact;
no Stage 1/2 code paths were modified. Stage 3 is purely additive:
the streaming code lives in `pipeline/stream_ingest.py` (~180 LOC),
the config gained one block, `run_all.py` gained six lines, and the
ADR + README notes round it out. Roughly 95% of the Stage 1/2
codebase passes through Stage 3 unchanged.

---

## Decision 2: What design decisions in Stage 1 would I change in hindsight?

I would change three things specifically.

**Pull the JSON schema out of `ingest.py` into `config/schemas.py`.**
At Stage 2 I put `TRANSACTION_SCHEMA` next to the ingest function
because that was the only place it was used. At Stage 3 the streaming
module needs the identical schema, so I reach across module
boundaries to import it
(`from pipeline.ingest import TRANSACTION_SCHEMA`). It works, but
`ingest.py` is now an unintentional schema authority. A `schemas.py`
that both `ingest.py` and `stream_ingest.py` import from would have
been the cleaner ownership.

**Make `_parse_date_flex` and `_is_nonstandard_date` public in a
`pipeline/_common.py`.** Right now `stream_ingest.py` imports
`_parse_date_flex` from `transform.py` despite the leading
underscore that conventionally signals private. Either rename
without the underscore or move them to a `_common.py` that both
modules depend on. The underscore was correct for the Stage 1
intra-module use; it became a small lie in Stage 3.

**Design `run_all.py` around a list of pipeline phases instead of an
inline call sequence.** A
`PHASES = [run_ingestion, run_transformation, run_provisioning, run_stream_ingestion]`
driven by a loop, with each phase declaring its own config
dependencies, would mean Stage 3's addition is a single list entry
rather than a free-standing conditional. Cosmetic at this size, but
at five+ phases it would matter, and it would make per-phase timing
and error-handling uniform rather than written separately for each
call site.

---

## Decision 3: How would I approach this differently if I had known Stage 3 was coming from the start?

If I had known Stage 3 was coming, the largest change would be
**treating the ingestion layer as source-agnostic from the start**.
Today `ingest.py` knows specifically about three input files at
three fixed paths; `stream_ingest.py` repeats that pattern for the
stream directory. With Day-1 visibility I would have written a
single `Source` abstraction (path, format, schema, watermark column)
and let both batch and stream pipelines compose `Source` instances.
Practically: `Source(path=..., format="jsonl", schema=...)`
produces a Spark DataFrame regardless of whether path is a file or
a directory; the difference between batch and stream becomes a
question of *how often the source is read*, not *which module knows
how to read it*.

Second, **the Gold layer would have been one module, not two**.
Today I have a `gold/` output written by `provision.py` and a
`stream_gold/` output written by `stream_ingest.py`. They are
genuinely related — `current_balances` is conceptually a real-time
view of `dim_accounts.current_balance` walked forward through events.
A unified Gold module with a `BalanceTable` class that exposes both
the snapshot view (rebuild from batch) and the incremental view
(merge per micro-batch) would have produced one source of truth for
balance arithmetic; today the CREDIT/REVERSAL increase rule lives in
a constant in `stream_ingest.py` and could conceivably drift from
batch semantics.

Third, **state management for `current_balances` would be Delta
MERGE from the start, not retrofitted**. The Stage 2 batch pipeline
uses Delta append + read patterns; Stage 3 needs MERGE semantics
which I introduced for the first time in `stream_ingest.py`. Knowing
this from Day 1, I would have built `provision.py`'s `dim_accounts`
write as a MERGE too (idempotent reruns, clean intermediate state)
even though Stage 1 did not strictly require it — the streaming
extension would then be a re-application of an already-proven
pattern instead of a new one.

Finally, **a single `python -m pipeline` entry point with a `--mode`
argument** (`batch`, `stream`, or `all`) instead of one
`run_all.py`. The scoring system always runs the full pipeline so
the practical difference is small, but for development iteration —
re-running just the streaming pass without rebuilding the batch
output — the mode flag pays for itself within the first day.

---

## Appendix

Diff sizes between stages, against the prior tag:

| Diff | Files | +/− LOC |
|---|---|---|
| `stage1-submission` → `stage2-submission` | 7 | +395 / −44 |
| `stage2-submission` → `stage3-submission` | 5 | additive (~250 / ~6) |

Stage 3 additions are concentrated in `pipeline/stream_ingest.py`
(the new module), this ADR, plus six lines in `pipeline/run_all.py`
and one config block in `config/pipeline_config.yaml`. No batch
code paths were modified.
