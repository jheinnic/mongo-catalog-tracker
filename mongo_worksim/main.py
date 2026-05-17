#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pymongo>=3.12,<5.0",
#     "pyyaml>=6.0.3",
# ]
# ///

"""
main.py  –  MongoDB collection/index load-test simulator

Usage
─────
    python main.py [options] <plan_dir> <output_dir>

Positional arguments
────────────────────
    plan_dir    Root directory where all pre-rolled plan files are stored.
    output_dir  Root directory for keyhole output sub-directories.
                Each capture lands in a sub-directory named
                <days_since_epoch>_<seconds_past_midnight>.

Required options
────────────────
    --mongo-uri   <uri>   MongoDB connection URI
    --keyhole-url <url>   URL passed verbatim to  keyhole --index <url>
    --names-file  <path>  Text file with bootstrap collection names (one per line)

Key optional options
────────────────────
    --config       <path>    sim_params.yaml to load  (default: sim_params.yaml
                             in the same directory as this script)
    --name-pattern <pattern> mktemp-style pattern overriding the one in the YAML.
                             Each uppercase X is replaced by a random [a-z0-9]
                             character.  Example: "run1_XXXXXXXXXX"
    --seed         <int>     RNG seed for reproducibility
    --workers      <int>     Thread-pool size  (overrides YAML; default: 16)
    --db-name      <str>     MongoDB database name  (overrides YAML)
    --skip-bootstrap         Skip collection creation phase
    --skip-preroll           Skip plan generation phase
    --start-interval <int>   Resume from this interval  (default: 1)
    --log-level    <str>     DEBUG | INFO | WARNING | ERROR  (default: WARNING)

Simulation overview
───────────────────
  Phase 1 – Bootstrap
      Create all bootstrap collections in parallel, then pause for the operator
      to restart MongoDB so they pre-exist the instance.

  Phase 2 – Pre-roll
      Simulate all N intervals' decisions up-front and write one JSON plan file
      per interval into plan_dir, along with supporting reference files.

  Phase 3 – Execute
      Run keyhole before interval 1, then for each interval:
        • Process the Active1–Active4 aging pipeline
        • Create and exercise ~625 new collections
        • Activate bootstrap and forgotten collections
        • Delete collections from the retirement sublist
        • Run keyhole after the interval completes
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import sys
import time

import pymongo
import pymongo.errors

try:
    import yaml
except ImportError:
    yaml = None  # handled below with a clear error message

from executor import execute_interval, run_keyhole
from mongo_ops import create_collection
from preroll import preroll


# ── config loading ────────────────────────────────────────────────────────────

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_CONFIG = os.path.join(_SCRIPT_DIR, "sim_params.yaml")


def _load_params(config_path: str) -> dict:
    """Load and return the YAML parameter file."""
    if yaml is None:
        print("ERROR: PyYAML is not installed.  Run:  pip install pyyaml")
        sys.exit(1)
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path) as fh:
        params = yaml.safe_load(fh)
    return params


def _apply_cli_overrides(params: dict, args: argparse.Namespace) -> dict:
    """Overlay any CLI flags that override YAML values."""
    if args.name_pattern is not None:
        params["name_pattern"] = args.name_pattern
    if args.workers is not None:
        params["workers"] = args.workers
    if args.db_name is not None:
        params["db_name"] = args.db_name
    return params


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_names(path: str) -> list[str]:
    with open(path) as fh:
        names = [ln.strip() for ln in fh if ln.strip()]
    if not names:
        print(f"ERROR: names file is empty: {path}")
        sys.exit(1)
    print(f"  Loaded {len(names):,} bootstrap names from {path}")
    return names


def _get_db(mongo_uri: str, db_name: str):
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5_000)
    client.admin.command("ping")
    return client[db_name]


def _bootstrap(
    mongo_uri: str,
    db_name: str,
    names: list[str],
    workers: int,
) -> None:
    """Create all bootstrap collections in parallel with progress reporting."""
    print(f"\n{'=' * 68}")
    print(f"  BOOTSTRAP – creating {len(names):,} collections")
    print(f"{'=' * 68}")

    db = _get_db(mongo_uri, db_name)
    total   = len(names)
    counter = [0]

    def _create(name: str) -> None:
        create_collection(db, name)
        counter[0] += 1
        n = counter[0]
        if n % 100 == 0 or n == total:
            print(f"    {n:>5,}/{total:,} created", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_create, n): n for n in names}
        for fut in concurrent.futures.as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                print(f"    !! create error '{futs[fut]}': {exc}")

    print(f"\n  Bootstrap complete – {total:,} collections created.")


def _pause_for_restart(mongo_uri: str, db_name: str) -> None:
    """Prompt the operator to restart MongoDB, then verify reconnection."""
    print("\n" + "=" * 68)
    print("  ⚠️   BOOTSTRAP COMPLETE")
    print()
    print("  Please RESTART MongoDB now so that the bootstrap collections")
    print("  pre-exist the instance.")
    print()
    print("  Press ENTER once MongoDB is back up …")
    print("=" * 68)
    input()

    print("\n  Verifying MongoDB connection", end="", flush=True)
    for _ in range(15):
        try:
            db = _get_db(mongo_uri, db_name)
            count = len(db.list_collection_names())
            print(f"\n  ✓  Connected – {count:,} collections visible.")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(2)

    print("\n  ✗  Could not reconnect to MongoDB after restart. Exiting.")
    sys.exit(1)


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # positional
    parser.add_argument("plan_dir",
                        help="Directory for pre-rolled plan files")
    parser.add_argument("output_dir",
                        help="Root directory for keyhole output captures")

    # required
    parser.add_argument("--mongo-uri",   required=True,
                        help="MongoDB connection URI")
    parser.add_argument("--keyhole-url", required=True,
                        help="URL argument for  keyhole --index <url>")
    parser.add_argument("--names-file",  required=True,
                        help="Bootstrap collection names, one per line")

    # config
    parser.add_argument("--config", default=_DEFAULT_CONFIG, metavar="PATH",
                        help=f"sim_params.yaml path  (default: {_DEFAULT_CONFIG})")
    parser.add_argument("--name-pattern", default=None, metavar="PATTERN",
                        help="mktemp-style name pattern, e.g. 'run1_XXXXXXXXXX' "
                             "(overrides YAML name_pattern)")

    # CLI overrides for common YAML values
    parser.add_argument("--db-name",  default=None,
                        help="MongoDB database name  (overrides YAML db_name)")
    parser.add_argument("--workers",  type=int, default=None,
                        help="Parallel thread count  (overrides YAML workers)")
    parser.add_argument("--seed",     type=int, default=None,
                        help="RNG seed for reproducibility")

    # execution control
    parser.add_argument("--skip-bootstrap",  action="store_true",
                        help="Skip collection creation phase")
    parser.add_argument("--skip-preroll",    action="store_true",
                        help="Skip plan generation phase (plan files must exist)")
    parser.add_argument("--start-interval",  type=int, default=1,
                        help="Resume from this interval number  (default: 1)")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # ── load and merge params ─────────────────────────────────────────
    print(f"\n  Loading config: {args.config}")
    params = _load_params(args.config)
    params = _apply_cli_overrides(params, args)

    workers  = params.get("workers",  16)
    db_name  = params.get("db_name",  "loadtest")
    n_itvs   = params.get("n_intervals", 40)

    print(f"  name_pattern : {params['name_pattern']}")
    print(f"  n_intervals  : {n_itvs}")
    print(f"  n_expansion  : {params['n_expansion']:,}")
    print(f"  workers      : {workers}")
    print(f"  db_name      : {db_name}")

    os.makedirs(args.plan_dir,   exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    names = _load_names(args.names_file)

    # ── Phase 1: Bootstrap ────────────────────────────────────────────
    if not args.skip_bootstrap:
        _bootstrap(args.mongo_uri, db_name, names, workers)
        _pause_for_restart(args.mongo_uri, db_name)
    else:
        print("  (--skip-bootstrap: skipping collection creation)")

    # ── Phase 2: Pre-roll ─────────────────────────────────────────────
    if not args.skip_preroll:
        print(f"\n{'=' * 68}")
        print(f"  PRE-ROLL – generating {n_itvs} interval plans")
        print(f"{'=' * 68}")
        preroll(names, args.plan_dir, params=params, seed=args.seed)
    else:
        print("  (--skip-preroll: using existing plan files)")
        missing = [
            f"interval_{i:02d}.json"
            for i in range(args.start_interval, n_itvs + 1)
            if not os.path.exists(
                os.path.join(args.plan_dir, f"interval_{i:02d}.json")
            )
        ]
        if missing:
            print(f"\n  ✗  Missing plan files: {missing[:5]}"
                  f"{'…' if len(missing) > 5 else ''}")
            sys.exit(1)

    # ── Phase 3: Execute ──────────────────────────────────────────────
    print(f"\n{'=' * 68}")
    print(f"  EXECUTION – running intervals {args.start_interval}–{n_itvs}")
    print(f"{'=' * 68}")

    try:
        db = _get_db(args.mongo_uri, db_name)
    except Exception as exc:
        print(f"\n  ✗  Cannot connect to MongoDB: {exc}")
        sys.exit(1)

    first = args.start_interval
    initial_label = (f"before_interval_{first:02d}"
                     if first > 1 else "before_interval_01_initial")
    _now       = datetime.datetime.now()
    start_day  = (_now.date() - datetime.date(1970, 1, 1)).days - n_itvs
    start_secs = _now.hour * 3600 + _now.minute * 60 + _now.second

    run_keyhole(args.keyhole_url, args.output_dir, label=initial_label,
                virtual_day=start_day, virtual_secs=start_secs)

    for interval in range(first, n_itvs + 1):
        plan_path = os.path.join(args.plan_dir, f"interval_{interval:02d}.json")
        if not os.path.exists(plan_path):
            print(f"\n  ✗  Plan file not found: {plan_path}")
            sys.exit(1)

        with open(plan_path) as fh:
            plan = json.load(fh)

        execute_interval(db=db, plan=plan, workers=workers)

        run_keyhole(
            args.keyhole_url,
            args.output_dir,
            label=f"after_interval_{interval:02d}",
            virtual_day=start_day + (interval - first + 1),
            virtual_secs=start_secs,
        )

    # ── Done ──────────────────────────────────────────────────────────
    executed = n_itvs - first + 1
    print(f"\n{'=' * 68}")
    print(f"  ✓  Simulation complete – {executed} interval(s) executed.")
    print(f"     Config     : {args.config}")
    print(f"     Plan files : {args.plan_dir}")
    print(f"     keyhole out: {args.output_dir}")
    print(f"{'=' * 68}\n")


if __name__ == "__main__":
    main()
