"""
preroll.py  –  Simulate all 40 intervals up-front, emitting one JSON plan file
               per interval into plan_dir.  No MongoDB traffic is produced here.

State that flows between intervals
───────────────────────────────────
  bootstrap_candidate   – bootstrap names not currently in any Active list
  forgotten_created     – created names dropped from the pipeline (still exist
                          in MongoDB; eligible for re-activation)
  active1 … active4     – aging pipeline lists, each entry is
                          {"name": str, "origin": "bootstrap"|"created"}
  pool_25000            – names not yet used from the expansion pool
  deletion_pool         – remaining names from the 380-name sublist

Promotion fractions (per-interval, per-stage)
─────────────────────────────────────────────
  Active1 → Active2 : ignore 33 %, promote 62 %, delete 5 % of created in promo
  Active2 → Active3 : ignore 70 %, promote 25 %, delete 5 % of created in promo
  Active3 → Active4 : ignore 90 %, promote  5 %, delete 5 % of created in promo
  Active4 → pool    : return all (no deletion)

The 5 % deletion applies ONLY to created-origin items in the promote group.
"""
from __future__ import annotations

import json
import math
import os
import random
from typing import Dict, List, Set, Tuple

# ── constants ─────────────────────────────────────────────────────────────────

N_INTERVALS   = 40
N_BOOTSTRAP   = 1500
N_DELETION    = 380       # sublist of bootstrap to delete over time
N_EXPANSION   = 25_000
TARGET_CREATE = 625       # new collections created per interval
CREATE_SIGMA  = 12
CREATE_MIN    = 600
CREATE_MAX    = 650

BS_ACTIVATE_MU, BS_ACTIVATE_SIGMA = 150, 25
BS_ACTIVATE_MIN, BS_ACTIVATE_MAX  = 100, 200

REACT_MU, REACT_SIGMA = 100, 25
REACT_MIN, REACT_MAX  = 50, 150

DEL_380_MU, DEL_380_SIGMA = 9, 2
DEL_380_MIN, DEL_380_MAX  = 3, 15

DELETE_FRAC_CREATED = 0.05   # fraction of created-origin in promote group to delete

STAGE_IGNORE_FRAC = {        # fraction of each active list that is ignored
    "active1": 0.33,
    "active2": 0.70,
    "active3": 0.90,
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _gi(mu: float, sigma: float, lo: int, hi: int) -> int:
    """Sample a clipped Gaussian integer."""
    return max(lo, min(hi, int(round(random.gauss(mu, sigma)))))


def _gen_expansion_names(existing: Set[str], count: int) -> List[str]:
    """Generate *count* unique hex-prefixed names not in *existing*."""
    names: List[str] = []
    seen = set(existing)
    while len(names) < count:
        candidate = "lt_" + format(random.getrandbits(48), "012x")
        if candidate not in seen:
            seen.add(candidate)
            names.append(candidate)
    return names


def _process_stage(
    entries: List[Dict],
    ignore_frac: float,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Split *entries* into (to_promote, to_delete, to_ignore).

    Ignore *ignore_frac* of the list.
    From the surviving promote group, mark ~5 % of created-origin for deletion
    instead of promotion.

    Returns
    -------
    to_promote : entries that move to the next active stage (queried in executor)
    to_delete  : created-origin entries to be queried then dropped permanently
    to_ignore  : entries that fall out of the pipeline (returned/forgotten)
    """
    n = len(entries)
    if n == 0:
        return [], [], []

    shuffled = random.sample(entries, n)          # deterministic shuffle with seed
    n_ignore  = round(n * ignore_frac)
    to_ignore  = shuffled[:n_ignore]
    promo_grp  = shuffled[n_ignore:]

    # 5 % of created-origin in promote group → permanent deletion
    created_promo = [e for e in promo_grp if e["origin"] == "created"]
    n_delete = int(len(created_promo) * DELETE_FRAC_CREATED)
    to_delete: List[Dict] = []
    if n_delete > 0 and created_promo:
        to_delete = random.sample(created_promo, n_delete)

    delete_names = {e["name"] for e in to_delete}
    to_promote = [e for e in promo_grp if e["name"] not in delete_names]

    return to_promote, to_delete, to_ignore


def _return_ignored(
    entries: List[Dict],
    bootstrap_candidate: Set[str],
    forgotten_created:   List[str],
) -> None:
    """Route ignored entries back to the correct inactive pool."""
    for e in entries:
        if e["origin"] == "bootstrap":
            bootstrap_candidate.add(e["name"])
        else:
            forgotten_created.append(e["name"])


# ── main entry point ──────────────────────────────────────────────────────────

def preroll(names_1500: List[str], plan_dir: str, seed: int | None = None) -> None:
    """
    Generate all 40 interval plan files and write them to *plan_dir*.
    Also writes names_25000.json, deletion_380.json, and preroll_meta.json.
    """
    if seed is not None:
        random.seed(seed)

    os.makedirs(plan_dir, exist_ok=True)

    # ── generate 25 000 expansion names ──────────────────────────────
    print("  Generating 25,000 expansion names …")
    names_25000 = _gen_expansion_names(set(names_1500), N_EXPANSION)
    with open(os.path.join(plan_dir, "names_25000.json"), "w") as fh:
        json.dump(names_25000, fh)

    # ── select 380-name deletion sublist from bootstrap ───────────────
    deletion_380 = random.sample(names_1500, N_DELETION)
    with open(os.path.join(plan_dir, "deletion_380.json"), "w") as fh:
        json.dump(deletion_380, fh)

    # ── initialise state ──────────────────────────────────────────────
    bootstrap_candidate: Set[str]  = set(names_1500) - set(deletion_380)
    forgotten_created:   List[str] = []

    active1: List[Dict] = []
    active2: List[Dict] = []
    active3: List[Dict] = []
    active4: List[Dict] = []

    pool_25000:    List[str] = list(names_25000)
    deletion_pool: List[str] = list(deletion_380)

    # ── pre-distribute create counts to fit within 25 000 budget ─────
    create_counts = [_gi(TARGET_CREATE, CREATE_SIGMA, CREATE_MIN, CREATE_MAX)
                     for _ in range(N_INTERVALS)]
    total_creates = sum(create_counts)
    if total_creates > len(pool_25000):
        # Scale proportionally, keep within bounds, trim residual from the end
        factor = len(pool_25000) / total_creates
        create_counts = [max(CREATE_MIN, min(CREATE_MAX, round(c * factor)))
                         for c in create_counts]
        while sum(create_counts) > len(pool_25000):
            idx = max(range(N_INTERVALS), key=lambda i: create_counts[i])
            if create_counts[idx] > CREATE_MIN:
                create_counts[idx] -= 1
            else:
                break   # safety valve

    # ── simulate all 40 intervals ─────────────────────────────────────
    print(f"  Simulating {N_INTERVALS} intervals …\n"
          f"  {'Intv':>4}  {'New':>4}  {'BSAct':>5}  "
          f"{'React':>5}  {'Del380':>6}  "
          f"{'A1':>5}  {'A2':>5}  {'A3':>5}  {'A4':>5}")

    for ix in range(N_INTERVALS):
        interval = ix + 1
        plan: Dict = {"interval": interval}

        # ── 1. Active4 → return to pools (no DB op at execution time) ──
        a4_ret_bs  = [e["name"] for e in active4 if e["origin"] == "bootstrap"]
        a4_ret_cr  = [e["name"] for e in active4 if e["origin"] == "created"]
        plan["active4_return_bootstrap"] = a4_ret_bs
        plan["active4_return_created"]   = a4_ret_cr
        for name in a4_ret_bs:
            bootstrap_candidate.add(name)
        forgotten_created.extend(a4_ret_cr)
        active4 = []

        # ── 2. Active3 → Active4 ───────────────────────────────────────
        a3_promo, a3_del, a3_ign = _process_stage(active3, STAGE_IGNORE_FRAC["active3"])
        plan["active3_to_active4"] = a3_promo
        plan["active3_to_delete"]  = a3_del
        plan["active3_to_ignore"]  = a3_ign
        _return_ignored(a3_ign, bootstrap_candidate, forgotten_created)
        active4 = a3_promo
        active3 = []

        # ── 3. Active2 → Active3 ───────────────────────────────────────
        a2_promo, a2_del, a2_ign = _process_stage(active2, STAGE_IGNORE_FRAC["active2"])
        plan["active2_to_active3"] = a2_promo
        plan["active2_to_delete"]  = a2_del
        plan["active2_to_ignore"]  = a2_ign
        _return_ignored(a2_ign, bootstrap_candidate, forgotten_created)
        active3 = a2_promo
        active2 = []

        # ── 4. Active1 → Active2 ───────────────────────────────────────
        a1_promo, a1_del, a1_ign = _process_stage(active1, STAGE_IGNORE_FRAC["active1"])
        plan["active1_to_active2"] = a1_promo
        plan["active1_to_delete"]  = a1_del
        plan["active1_to_ignore"]  = a1_ign
        _return_ignored(a1_ign, bootstrap_candidate, forgotten_created)
        active2 = a1_promo
        active1 = []

        # ── 5a. New creates from 25 000 pool ───────────────────────────
        n_create  = min(create_counts[ix], len(pool_25000))
        new_creates = pool_25000[:n_create]
        pool_25000  = pool_25000[n_create:]
        plan["new_creates"] = new_creates

        # ── 5b. Bootstrap activations ──────────────────────────────────
        n_bs = min(_gi(BS_ACTIVATE_MU, BS_ACTIVATE_SIGMA,
                       BS_ACTIVATE_MIN, BS_ACTIVATE_MAX),
                   len(bootstrap_candidate))
        bs_activate = (random.sample(sorted(bootstrap_candidate), n_bs)
                       if n_bs > 0 else [])
        for name in bs_activate:
            bootstrap_candidate.discard(name)
        plan["bootstrap_activate"] = bs_activate

        # ── 5c. Forgotten re-activations (interval 2 onward) ──────────
        if interval > 1 and forgotten_created:
            n_react = min(_gi(REACT_MU, REACT_SIGMA, REACT_MIN, REACT_MAX),
                          len(forgotten_created))
            react = random.sample(forgotten_created, n_react) if n_react > 0 else []
            for name in react:
                forgotten_created.remove(name)
        else:
            react = []
        plan["forgotten_reactivate"] = react

        # ── 5d. Build new Active1 ──────────────────────────────────────
        active1 = (
            [{"name": n, "origin": "created"}   for n in new_creates]
          + [{"name": n, "origin": "bootstrap"} for n in bs_activate]
          + [{"name": n, "origin": "created"}   for n in react]
        )

        # ── 6. 380-sublist deletions (normal dist, μ=9, σ=2) ──────────
        n_del = min(_gi(DEL_380_MU, DEL_380_SIGMA, DEL_380_MIN, DEL_380_MAX),
                    len(deletion_pool))
        del_380 = random.sample(deletion_pool, n_del) if n_del > 0 else []
        for name in del_380:
            deletion_pool.remove(name)
        plan["delete_380"] = del_380

        # ── stats snapshot ─────────────────────────────────────────────
        plan["_stats"] = {
            "active1_size_after":            len(active1),
            "active2_size_after":            len(active2),
            "active3_size_after":            len(active3),
            "active4_size_after":            len(active4),
            "bootstrap_candidate_remaining": len(bootstrap_candidate),
            "forgotten_created_remaining":   len(forgotten_created),
            "pool_25000_remaining":          len(pool_25000),
            "deletion_pool_remaining":       len(deletion_pool),
        }

        path = os.path.join(plan_dir, f"interval_{interval:02d}.json")
        with open(path, "w") as fh:
            json.dump(plan, fh, indent=2)

        s = plan["_stats"]
        print(f"  {interval:>4}  {n_create:>4}  {len(bs_activate):>5}  "
              f"{len(react):>5}  {n_del:>6}  "
              f"{s['active1_size_after']:>5}  {s['active2_size_after']:>5}  "
              f"{s['active3_size_after']:>5}  {s['active4_size_after']:>5}")

    # ── overall metadata ──────────────────────────────────────────────
    meta = {
        "n_intervals":                   N_INTERVALS,
        "seed":                          seed,
        "deletion_380_count":            len(deletion_380),
        "expansion_pool_consumed":       N_EXPANSION - len(pool_25000),
        "expansion_pool_remaining":      len(pool_25000),
        "final_bootstrap_candidate":     len(bootstrap_candidate),
        "final_forgotten_created":       len(forgotten_created),
        "final_deletion_pool_remaining": len(deletion_pool),
    }
    with open(os.path.join(plan_dir, "preroll_meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"\n  Pre-roll complete – {N_INTERVALS} plans written to: {plan_dir}")
