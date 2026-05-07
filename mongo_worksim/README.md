# MongoDB Load-Test Simulator

Simulates realistic collection and index activity across a MongoDB 4.0 instance
over a configurable number of intervals, capturing a `keyhole --index` report
before the first interval and after every completed one.

---

## Requirements

* Python 3.10+
* `keyhole` on `$PATH`
* MongoDB 4.0 accessible at the URI you supply

```bash
pip install -r requirements.txt
```

---

## Quick start

```bash
# 1. Prepare a bootstrap names file – one collection name per line
wc -l my_collection_names.txt    # typically 1500

# 2. (Optional) Edit sim_params.yaml to taste, or leave defaults as-is

# 3. Run the full pipeline
python main.py \
    --mongo-uri   "mongodb://localhost:27017" \
    --keyhole-url "mongodb://localhost:27017" \
    --names-file  my_collection_names.txt \
    /path/to/plan_dir \
    /path/to/output_dir
```

The script will:
1. Create all bootstrap collections in parallel.
2. **Pause and prompt you to restart MongoDB** so those collections pre-exist
   the instance before any simulation traffic begins.
3. Pre-roll all interval plans into `plan_dir`.
4. Run the intervals, emitting a keyhole capture before the first and after
   each completed one.

---

## Arguments

### Positional

| Argument | Description |
|---|---|
| `plan_dir` | Root directory for pre-rolled plan JSON files |
| `output_dir` | Root directory for keyhole output sub-directories |

### Required

| Flag | Description |
|---|---|
| `--mongo-uri <uri>` | MongoDB connection URI, e.g. `mongodb://localhost:27017` |
| `--keyhole-url <url>` | URL passed verbatim to `keytool --index <url>` |
| `--names-file <path>` | File containing bootstrap collection names, one per line |

### Optional

| Flag | Default | Description |
|---|---|---|
| `--config <path>` | `sim_params.yaml` beside the script | Parameter file to load (see below) |
| `--name-pattern <pattern>` | *(from YAML)* | mktemp-style name pattern, e.g. `"run1_XXXXXXXXXX"` — overrides `name_pattern` in the YAML without editing it |
| `--db-name <str>` | *(from YAML)* | MongoDB database name |
| `--workers <int>` | *(from YAML)* | Parallel thread count for MongoDB operations |
| `--seed <int>` | *(none)* | RNG seed for fully reproducible runs |
| `--skip-bootstrap` | — | Skip collection creation (collections already exist) |
| `--skip-preroll` | — | Skip plan generation (plan files must already exist) |
| `--start-interval <int>` | `1` | Resume from this interval number |
| `--log-level` | `WARNING` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

---

## Configuration: `sim_params.yaml`

All tunable simulation parameters live in `sim_params.yaml`.  The file ships
with documented defaults matching the original specification.  The effective
values used by a run are printed at startup so there is no ambiguity.

```yaml
# mktemp-style pattern for auto-generated expansion-pool names.
# Each uppercase X → one random character from [a-z0-9].
# The number of X's sets the suffix length.
# With 8 X's: 36^8 ≈ 2.8 trillion combinations.
name_pattern: "lt_XXXXXXXX"

n_expansion:  25000    # expansion pool size
n_deletion:     380    # bootstrap names reserved for retirement deletions
n_intervals:     40    # number of simulation intervals

target_create_mu:    625   # new collections created per interval …
target_create_sigma:  12   # … (Gaussian, clipped to min/max)
target_create_min:   600
target_create_max:   650

bs_activate_mu:    150     # bootstrap activations per interval
bs_activate_sigma:  25
bs_activate_min:   100
bs_activate_max:   200

reactivate_mu:    100      # forgotten-pool re-activations per interval
reactivate_sigma:  25      # (begins at interval 2)
reactivate_min:    50
reactivate_max:   150

del_sublist_mu:     9      # retirement-sublist deletions per interval
del_sublist_sigma:  2      # (normal distribution, μ=9 → ~360 total over 40 intervals)
del_sublist_min:    3
del_sublist_max:   15

stage_ignore_frac:         # fraction of each active list that is *ignored*
  active1: 0.33            # 33 % ignored → 62 % promote, 5 % hard-deleted
  active2: 0.70            # 70 % ignored → 25 % promote, 5 % hard-deleted
  active3: 0.90            # 90 % ignored →  5 % promote, 5 % hard-deleted

delete_frac_created: 0.05  # fraction of created-origin items in each stage's
                           # promote group to query-then-delete permanently

workers:  16               # default thread-pool size
db_name: "loadtest"        # default MongoDB database name
```

Any value can be overridden on the command line where a matching flag exists
(`--name-pattern`, `--workers`, `--db-name`).

### Name pattern

The `name_pattern` field (and its `--name-pattern` CLI override) follows the
`mktemp(1)` convention: every uppercase `X` is replaced by a single random
character from `[a-z0-9]`.  The pattern itself determines the suffix length,
so no separate length parameter is needed.

```
"lt_XXXXXXXX"    →  "lt_3a7fb2c9"   (8-char suffix, 36^8 ≈ 2.8T combinations)
"run1_XXXXXXXXXX" →  "run1_q7z2m0cs4a" (10-char suffix)
"col_XXXXXX"     →  "col_8fj3dt"    (6-char suffix, 36^6 ≈ 2.2B combinations)
```

The pre-roller validates that the pattern has sufficient capacity (at least 4×
headroom over `n_expansion`) and raises a clear error if not.

---

## Output directories

Each `keyhole --index` capture is written to a sub-directory of `output_dir`
named:

```
<days_since_Unix_epoch>_<seconds_past_midnight>
```

e.g. `20201_10000`.  Directories appear in chronological order, so a plain
`ls` or `sort` produces the correct sequence.  Contents of each directory:

| File | Description |
|---|---|
| `stdout.txt` | Raw keyhole stdout |
| `stderr.txt` | keyhole stderr (if any) |
| `meta.json` | Label, return code, UTC timestamp |

There is one capture **before** interval 1 (labelled `before_interval_01_initial`)
and one **after** each of the 40 intervals, for 41 captures total.

---

## Plan files

`preroll.py` generates one JSON file per interval in `plan_dir`:

```
plan_dir/
  interval_01.json    ← complete decision set for interval 1
  interval_02.json
  …
  interval_40.json
  names_expansion.json     ← generated expansion-pool names (reference)
  deletion_sublist.json    ← retirement-deletion sublist (reference)
  preroll_meta.json        ← overall simulation metadata and pool statistics
```

Each interval file is fully self-contained: the executor reads it and performs
exactly the operations it describes, making the run deterministic against any
pre-rolled plan.  The `_stats` block at the end of each file records pool sizes
after that interval for inspection or debugging.

---

## Collection schema

Every collection is created with **5 documents**:

```json
{
  "_id":     ObjectId,
  "label":   "<coll_name_prefix>_doc<i>",
  "score":   <int 1–1000>,
  "tag":     <one of 10 Greek letter words>,
  "created": ISODate,
  "seq":     <0–4>
}
```

An ascending index named `score_idx` is created on the `score` field.

The three queries used to raise a collection's index OpCount:

```javascript
db[name].find({ score: { $gt:  250 } }).limit(10)
db[name].find({ score: { $lt:  750 } }).limit(10)
db[name].find({ score: { $gte: 100, $lte: 900 } }).limit(10)
```

---

## Simulation design

### Active pipeline (persists across intervals)

Items age through four stages over consecutive intervals:

```
interval N    → item enters Active1
interval N+1  → 33 % ignored, 62 % → Active2, 5 % of created queried+deleted
interval N+2  → 70 % ignored, 25 % → Active3, 5 % of created queried+deleted
interval N+3  → 90 % ignored,  5 % → Active4, 5 % of created queried+deleted
interval N+4  → all returned to candidate pools (no DB op)
```

The **5 % hard-delete** at each promotion step targets **only created-origin**
items (those drawn from the expansion pool).  Bootstrap-origin items are never
permanently deleted through the pipeline.

### Ignored-item routing

| Origin | Destination when ignored |
|---|---|
| Bootstrap (original names file) | `bootstrap_candidate` pool — eligible for re-activation |
| Created (expansion pool) | `forgotten_created` pool — still live in MongoDB; eligible for re-activation from interval 2 onward |

### Per-interval inputs to Active1

| Source | Count distribution |
|---|---|
| New creates from expansion pool | Gaussian μ=625, σ=12, clipped 600–650 |
| Bootstrap candidate activations | Gaussian μ=150, σ=25, clipped 100–200 |
| Forgotten re-activations | Gaussian μ=100, σ=25, clipped 50–150 (interval 2+) |

All distribution parameters are configurable in `sim_params.yaml`.

### Retirement-sublist deletions

A sub-sample of `n_deletion` bootstrap names is chosen at pre-roll time.
Each interval, a Gaussian sample (μ=9, σ=2, clipped 3–15) of these are
permanently dropped from MongoDB.  The μ=9 average over 40 intervals yields
~360 total deletions, safely within the 380-name budget, with the tail covered
by the normal distribution's natural variance.

### Pre-roll and padding

All 40 interval plans are generated before execution begins.  Create counts
across intervals are pre-distributed to ensure the expansion pool is never
over-drawn; any remainder stays as padding that the last intervals can draw on
if earlier intervals ran slightly high.

---

## Resuming a run

If a run is interrupted after bootstrap and pre-roll are complete:

```bash
python main.py \
    --skip-bootstrap \
    --skip-preroll \
    --start-interval 17 \
    --mongo-uri   "mongodb://localhost:27017" \
    --keyhole-url "mongodb://localhost:27017" \
    --names-file  my_collection_names.txt \
    /path/to/plan_dir \
    /path/to/output_dir
```

The keyhole capture labelled `before_interval_17` is emitted immediately before
interval 17 begins, maintaining the unbroken before/after sequence.

---

## Files at a glance

| File | Role |
|---|---|
| `main.py` | CLI entry point — orchestrates all three phases |
| `preroll.py` | Pre-rolls all interval plans; generates expansion names |
| `executor.py` | Executes one plan file against MongoDB; runs keyhole |
| `mongo_ops.py` | `create_collection`, `query_collection`, `delete_collection` and helpers |
| `sim_params.yaml` | All tunable simulation parameters |
| `requirements.txt` | Python dependencies (`pymongo`, `pyyaml`) |
