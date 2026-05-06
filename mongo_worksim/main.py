#!/usr/bin/env python3
"""
main.py  –  MongoDB collection/index load-test simulator

Usage
─────
    python main.py [options] <plan_dir> <output_dir>

Positional arguments
────────────────────
    plan_dir    Root directory where all pre-rolled plan files are stored.
    output_dir  Root directory where keyhole output subdirectories are written.
                Each keyhole capture lands in a subdirectory named
                <days_since_epoch>_<seconds_past_midnight>.

Required options
────────────────
    --mongo-uri   <uri>   MongoDB connection URI
                          e.g. mongodb://localhost:27017
    --keyhole-url <url>   URL passed to  keyhole --index <url>
    --names-file  <path>  Text file with exactly 1500 collection names,
                          one per line.

Optional options
────────────────
    --db-name         <str>   MongoDB database name  (default: loadtest)
    --workers         <int>   Thread-pool size for parallel ops  (default: 16)
    --seed            <int>   Random seed for reproducibility
    --skip-bootstrap          Skip bootstrap phase (collections already exist)
    --skip-preroll            Skip pre-roll phase (plan files already exist)
    --start-interval  <int>   Resume from this interval  (default: 1)
    --log-level       <str>   DEBUG | INFO | WARNING | ERROR  (default: WARNING)

Simulation overview
───────────────────
  Phase 1 – Bootstrap
      Create all 1 500 bootstrap collections in parallel, then pause for the
      operator to restart MongoDB (so they pre-exist the instance).

  Phase 2 – Pre-roll
      Simulate all 40 intervals' decisions up-front and write one JSON plan
      file per interval into plan_dir.

  Phase 3 – Execute
      Run keyhole before interval 1, then for each of the 40 intervals:
        • Process the Active1–Active4 aging pipeline (queries / promotions /
          deletions) as specified in the pre-rolled plan.
        • Create and exercise ~625 new collections.
        • Activate 100–200 bootstrap and 50–150 forgotten collections.
        • Delete 3–15 collections from the 380-name retirement sublist.
        • Run keyhole after the interval completes.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time

import pymongo
import pymongo.errors

from executor import execute_interval, run_keyhole
from mongo_ops import create_collection
from preroll import preroll


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_names(path: str) -> list[str]:
    with open(path) as fh:
        names = [ln.strip() for ln in fh if ln.strip()]
    if len(names) != 1500:
        print(f"WARNING: expected 1500 names in {path}, found {len(names)}. "
              "Continuing anyway.")
    return names


def _get_db(mongo_uri: str, db_name: str):
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")   # fail fast if unreachable
    return client[db_name]


def _bootstrap(mongo_uri: str, db_name: str, names: list[str], workers: int) -> None:
    """Create all bootstrap collections in parallel with progress reporting."""
    print(f"\n{'=' * 68}")
    print(f"  BOOTSTRAP – creating {len(names)} collections")
    print(f"{'=' * 68}")

    db = _get_db(mongo_uri, db_name)
    total = len(names)
    counter = [0]

    def _create(name: str):
        create_collection(db, name)
        counter[0] += 1
        n = counter[0]
        if n % 100 == 0 or n == total:
            print(f"    {n:>5}/{total} created", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_create, n): n for n in names}
        for fut in concurrent.futures.as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                print(f"    !! create error '{futs[fut]}': {exc}")

    print(f"\n  Bootstrap complete – {total} collections created.")


def _pause_for_restart(mongo_uri: str, db_name: str) -> None:
    """Print instructions, wait for ENTER, then verify reconnection."""
    print("\n" + "=" * 68)
    print("  ⚠️   BOOTSTRAP COMPLETE")
    print()
    print("  Please RESTART MongoDB now so that the 1 500 collections")
    print("  pre-exist the instance.")
    print()
    print("  Press ENTER once MongoDB is back up …")
    print("=" * 68)
    input()

    print("\n  Verifying MongoDB connection", end="", flush=True)
    for attempt in range(15):
        try:
            db = _get_db(mongo_uri, db_name)
            count = len(db.list_collection_names())
            print(f"  ✓  Connected – {count} collections visible.")
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
                        help="Text file with 1500 collection names, one per line")

    # optional
    parser.add_argument("--db-name",   default="loadtest",
                        help="MongoDB database name  (default: loadtest)")
    parser.add_argument("--workers",   type=int, default=16,
                        help="Parallel worker threads  (default: 16)")
    parser.add_argument("--seed",      type=int, default=None,
                        help="Random seed for reproducibility")
    parser.add_argument("--skip-bootstrap", action="store_true",
                        help="Skip bootstrap phase")
    parser.add_argument("--skip-preroll",   action="store_true",
                        help="Skip pre-roll phase (plan files must already exist)")
    parser.add_argument("--start-interval", type=int, default=1,
                        help="Resume from this interval number  (default: 1)")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging verbosity  (default: WARNING)")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    os.makedirs(args.plan_dir,   exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    names = _load_names(args.names_file)

    # ── Phase 1: Bootstrap ────────────────────────────────────────────
    if not args.skip_bootstrap:
        _bootstrap(args.mongo_uri, args.db_name, names, args.workers)
        _pause_for_restart(args.mongo_uri, args.db_name)
    else:
        print("  (--skip-bootstrap: skipping collection creation)")

    # ── Phase 2: Pre-roll ─────────────────────────────────────────────
    if not args.skip_preroll:
        print(f"\n{'=' * 68}")
        print(f"  PRE-ROLL – generating all 40 interval plans")
        print(f"{'=' * 68}")
        preroll(names, args.plan_dir, seed=args.seed)
    else:
        print("  (--skip-preroll: using existing plan files)")
        # Sanity-check that the plan files are present
        missing = [
            f"interval_{i:02d}.json"
            for i in range(args.start_interval, 41)
            if not os.path.exists(
                os.path.join(args.plan_dir, f"interval_{i:02d}.json")
            )
        ]
        if missing:
            print(f"\n  ✗  Missing plan files: {missing[:5]} …")
            sys.exit(1)

    # ── Phase 3: Execute 40 intervals ────────────────────────────────
    print(f"\n{'=' * 68}")
    print(f"  EXECUTION – running intervals {args.start_interval}–40")
    print(f"{'=' * 68}")

    try:
        db = _get_db(args.mongo_uri, args.db_name)
    except Exception as exc:
        print(f"\n  ✗  Cannot connect to MongoDB: {exc}")
        sys.exit(1)

    n_intervals = 40
    first       = args.start_interval

    # Initial keyhole capture (before interval 1, or before the resume point)
    initial_label = (f"before_interval_{first:02d}"
                     if first > 1 else "before_interval_01_initial")
    run_keyhole(args.keyhole_url, args.output_dir, label=initial_label)

    for interval in range(first, n_intervals + 1):
        plan_path = os.path.join(args.plan_dir, f"interval_{interval:02d}.json")
        if not os.path.exists(plan_path):
            print(f"\n  ✗  Plan file not found: {plan_path}")
            sys.exit(1)

        with open(plan_path) as fh:
            plan = json.load(fh)

        execute_interval(db=db, plan=plan, workers=args.workers)

        run_keyhole(
            args.keyhole_url,
            args.output_dir,
            label=f"after_interval_{interval:02d}",
        )

    # ── Done ──────────────────────────────────────────────────────────
    executed = n_intervals - first + 1
    print(f"\n{'=' * 68}")
    print(f"  ✓  Simulation complete – {executed} interval(s) executed.")
    print(f"     Plan files : {args.plan_dir}")
    print(f"     keyhole out: {args.output_dir}")
    print(f"{'=' * 68}\n")


if __name__ == "__main__":
    main()
