"""
executor.py  –  Execute a pre-rolled interval plan against MongoDB and capture
                keyhole output.
"""
from __future__ import annotations

import concurrent.futures
import datetime
import json
import os
import subprocess
import logging
from typing import Callable, List

from mongo_ops import (
    create_collection,
    delete_collection,
    exercise_collection,
    query_collection,
    query_then_delete,
)

log = logging.getLogger(__name__)


# ── keyhole helpers ───────────────────────────────────────────────────────────

def _keyhole_dirname(days: int, secs: int) -> str:
    """
    Build a directory name of the form  <days_since_epoch>_<seconds_past_midnight>
    using zero-padded 5-digit fields.
    """
    return f"{days:05d}_{secs:05d}"


def run_keyhole(keyhole_url: str, output_root: str, label: str,
                virtual_day: int, virtual_secs: int) -> str:
    """
    Invoke ``keyhole --index <keyhole_url>``, write stdout / stderr / metadata
    into a timestamped sub-directory of *output_root*, and return that path.

    *virtual_day* and *virtual_secs* supply the simulated clock value for the
    directory name; the caller increments virtual_day by 1 per interval so that
    output directories reflect a day-per-interval cadence regardless of wall time.
    """
    dir_name = _keyhole_dirname(virtual_day, virtual_secs)
    out_dir  = os.path.join(output_root, dir_name)
    os.makedirs(out_dir, exist_ok=True)

    print(f"    [keyhole] {label:35s} → {dir_name}", flush=True)

    meta: dict = {
        "label":         label,
        "dir":           dir_name,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
    }

    try:
        result = subprocess.run(
            ["keyhole", "--index", keyhole_url],
            capture_output=True,
            text=True,
            timeout=180,
        )
        with open(os.path.join(out_dir, "stdout.txt"), "w") as fh:
            fh.write(result.stdout)
        if result.stderr:
            with open(os.path.join(out_dir, "stderr.txt"), "w") as fh:
                fh.write(result.stderr)
        meta["returncode"] = result.returncode
        if result.returncode != 0:
            log.warning("keyhole exited %d for label '%s'", result.returncode, label)
    except Exception as exc:
        meta["error"] = str(exc)
        log.error("keyhole failed (%s): %s", label, exc)

    with open(os.path.join(out_dir, "meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    return out_dir


# ── parallel execution helper ─────────────────────────────────────────────────

def _par(db, fn: Callable, names: List[str], workers: int, label: str = "") -> None:
    """
    Run ``fn(db, name)`` for every name in *names* using a thread pool.
    Exceptions are logged per-item rather than aborting the batch.
    """
    if not names:
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_name = {ex.submit(fn, db, name): name for name in names}
        errors = 0
        for fut in concurrent.futures.as_completed(future_to_name):
            try:
                fut.result()
            except Exception as exc:
                errors += 1
                log.warning("[%s] error on '%s': %s", label, future_to_name[fut], exc)
    if errors:
        print(f"      !! {errors} error(s) in batch '{label}'")


# ── interval execution ────────────────────────────────────────────────────────

def execute_interval(db, plan: dict, workers: int = 16) -> None:
    """
    Execute one pre-rolled interval plan against *db*.

    Parameters
    ----------
    db      : pymongo Database object
    plan    : dict loaded from interval_NN.json
    workers : thread-pool size for parallel collection ops
    """
    interval = plan["interval"]
    print(f"\n{'═' * 68}")
    print(f"  INTERVAL {interval:02d}")
    print(f"{'═' * 68}")

    # ── Active 4 → return to pools ────────────────────────────────────
    # Collections are still alive in MongoDB; we simply stop tracking them.
    a4_all = (plan.get("active4_return_bootstrap", [])
             + plan.get("active4_return_created",   []))
    if a4_all:
        print(f"  [A4 → pool]   {len(a4_all):5d} collections returned (no DB op)")

    # ── Active 3 → Active 4  (10 % survive) ──────────────────────────
    a3_promo = [e["name"] for e in plan.get("active3_to_active4", [])]
    a3_del   = [e["name"] for e in plan.get("active3_to_delete",  [])]
    a3_ign   = plan.get("active3_to_ignore", [])
    if a3_promo:
        print(f"  [A3 → A4]     {len(a3_promo):5d}  queried (promote)")
        _par(db, query_collection, a3_promo, workers, "A3→A4")
    if a3_del:
        print(f"  [A3 → del]    {len(a3_del):5d}  queried + deleted")
        _par(db, query_then_delete, a3_del, workers, "A3→del")
    if a3_ign:
        print(f"  [A3 → ignore] {len(a3_ign):5d}  returned/forgotten (no DB op)")

    # ── Active 2 → Active 3  (30 % survive) ──────────────────────────
    a2_promo = [e["name"] for e in plan.get("active2_to_active3", [])]
    a2_del   = [e["name"] for e in plan.get("active2_to_delete",  [])]
    a2_ign   = plan.get("active2_to_ignore", [])
    if a2_promo:
        print(f"  [A2 → A3]     {len(a2_promo):5d}  queried (promote)")
        _par(db, query_collection, a2_promo, workers, "A2→A3")
    if a2_del:
        print(f"  [A2 → del]    {len(a2_del):5d}  queried + deleted")
        _par(db, query_then_delete, a2_del, workers, "A2→del")
    if a2_ign:
        print(f"  [A2 → ignore] {len(a2_ign):5d}  returned/forgotten (no DB op)")

    # ── Active 1 → Active 2  (67 % survive) ──────────────────────────
    a1_promo = [e["name"] for e in plan.get("active1_to_active2", [])]
    a1_del   = [e["name"] for e in plan.get("active1_to_delete",  [])]
    a1_ign   = plan.get("active1_to_ignore", [])
    if a1_promo:
        print(f"  [A1 → A2]     {len(a1_promo):5d}  queried (promote)")
        _par(db, query_collection, a1_promo, workers, "A1→A2")
    if a1_del:
        print(f"  [A1 → del]    {len(a1_del):5d}  queried + deleted")
        _par(db, query_then_delete, a1_del, workers, "A1→del")
    if a1_ign:
        print(f"  [A1 → ignore] {len(a1_ign):5d}  returned/forgotten (no DB op)")

    # ── New Active 1 inputs ───────────────────────────────────────────

    new_creates = plan.get("new_creates", [])
    if new_creates:
        print(f"  [new]         {len(new_creates):5d}  created + exercised")
        _par(db, exercise_collection, new_creates, workers, "new-create")

    bs_activate = plan.get("bootstrap_activate", [])
    if bs_activate:
        print(f"  [bs-activate] {len(bs_activate):5d}  queried (bootstrap)")
        _par(db, query_collection, bs_activate, workers, "bs-activate")

    reactivate = plan.get("forgotten_reactivate", [])
    if reactivate:
        print(f"  [reactivate]  {len(reactivate):5d}  queried (forgotten→active)")
        _par(db, query_collection, reactivate, workers, "reactivate")

    # ── 380-sublist deletions ─────────────────────────────────────────
    del_380 = plan.get("delete_380", [])
    if del_380:
        print(f"  [380-delete]  {len(del_380):5d}  collections permanently deleted")
        _par(db, delete_collection, del_380, workers, "380-del")

    # ── interval summary ──────────────────────────────────────────────
    stats = plan.get("_stats", {})
    if stats:
        print(f"\n  Pipeline after this interval: "
              f"A1={stats.get('active1_size_after', '?'):>5}  "
              f"A2={stats.get('active2_size_after', '?'):>5}  "
              f"A3={stats.get('active3_size_after', '?'):>5}  "
              f"A4={stats.get('active4_size_after', '?'):>5}  "
              f"| bs_cand={stats.get('bootstrap_candidate_remaining', '?'):>5}  "
              f"forgotten={stats.get('forgotten_created_remaining', '?'):>5}")
