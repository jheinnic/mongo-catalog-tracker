# MongoDB Load-Test Simulator

Simulates realistic collection and index activity across a MongoDB 4.0 instance
in a 40-interval pipeline, capturing a `keyhole --index` report before the first
interval and after every completed interval.

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
# 1. Prepare a names file – one collection name per line, exactly 1500 lines
cat my_collection_names.txt | wc -l   # should print 1500

# 2. Run the full pipeline
python main.py \
    --mongo-uri   "mongodb://localhost:27017" \
    --keyhole-url "mongodb://localhost:27017" \
    --names-file  my_collection_names.txt \
    /path/to/plan_dir \
    /path/to/output_dir
```

The script will:
1. Create all 1 500 bootstrap collections.
2. **Pause and prompt you to restart MongoDB** (so the collections pre-exist the
   instance).
3. Pre-roll all 40 interval plans and write them to `plan_dir`.
4. Run the 40 intervals, emitting a keyhole capture before the first interval
   and after each completed one.

---

## Arguments

| Argument | Required | Description |
|---|---|---|
| `plan_dir` | ✓ | Root directory for pre-rolled plan JSON files |
| `output_dir` | ✓ | Root directory for keyhole output sub-directories |
| `--mongo-uri` | ✓ | MongoDB connection URI |
| `--keyhole-url` | ✓ | URL passed to `keyhole --index <url>` |
| `--names-file` | ✓ | File with 1 500 collection names, one per line |
| `--db-name` | | MongoDB database name (default: `loadtest`) |
| `--workers` | | Parallel thread count (default: `16`) |
| `--seed` | | Integer seed for reproducibility |
| `--skip-bootstrap` | | Skip collection creation (collections already exist) |
| `--skip-preroll` | | Skip plan generation (plan files already exist) |
| `--start-interval` | | Resume from this interval number (default: `1`) |
| `--log-level` | | `DEBUG` / `INFO` / `WARNING` / `ERROR` (default: `WARNING`) |

---

## Output directories

Each `keyhole --index` capture is written to a sub-directory of `output_dir`
named:

```
<days_since_Unix_epoch>_<seconds_past_midnight>
```

e.g. `20201_10000`.  Contents of each directory:

| File | Description |
|---|---|
| `stdout.txt` | Raw keyhole stdout |
| `stderr.txt` | keyhole stderr (if any) |
| `meta.json` | Label, return code, UTC timestamp |

Directories are created in chronological order so a simple `ls` sorts them
correctly.

---

## Plan files

`preroll.py` generates one JSON file per interval in `plan_dir`:

```
plan_dir/
  interval_01.json
  interval_02.json
  …
  interval_40.json
  names_25000.json      # expansion pool (reference)
  deletion_380.json     # 380-name retirement sublist
  preroll_meta.json     # overall simulation metadata
```

Each interval file contains the complete decision set for that interval
(which names to create, activate, promote, delete, etc.) so execution is
deterministic against any pre-rolled plan.

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

The three queries used to raise a collection's OpCount:

```javascript
db[name].find({ score: { $gt:  250 } }).limit(10)
db[name].find({ score: { $lt:  750 } }).limit(10)
db[name].find({ score: { $gte: 100, $lte: 900 } }).limit(10)
```

---

## Simulation design

### Active pipeline (persists across intervals)

```
interval N   → item enters Active1
interval N+1 → 33% ignored, 62% → Active2, 5% of created queried+deleted
interval N+2 → 70% ignored, 25% → Active3, 5% of created queried+deleted
interval N+3 → 90% ignored,  5% → Active4, 5% of created queried+deleted
interval N+4 → all returned to candidate pools (no DB op)
```

The **5% deletion** at each promotion step applies **only to created-origin**
items (those from the 25 000-name expansion pool), never to the original 1 500
bootstrap collections.

### Ignored-item routing

| Origin | Destination when ignored |
|---|---|
| Bootstrap (original 1 500) | `bootstrap_candidate` pool (eligible for re-activation) |
| Created (expansion pool) | `forgotten_created` pool (still in MongoDB; eligible for re-activation in a future interval) |

### Per-interval inputs to Active1

| Source | Count |
|---|---|
| New creates from 25 000 pool | ~625 ± 25 (Gaussian, σ=12, clipped 600–650) |
| Bootstrap candidate activations | 100–200 (Gaussian, μ=150, σ=25) |
| Forgotten re-activations | 50–150 (Gaussian, μ=100, σ=25) – interval 2 onward |

### 380-sublist deletions

A sub-sample of 380 bootstrap collection names is selected at pre-roll time.
Each interval, a Gaussian sample (μ=9, σ=2, clipped 3–15) of these are
permanently dropped, exhausting the list around interval 40 on average.

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
