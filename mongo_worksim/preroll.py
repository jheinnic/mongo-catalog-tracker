"""
preroll.py  –  Simulate all N intervals up-front, emitting one JSON plan file
               per interval into plan_dir.  No MongoDB traffic is produced here.

State that flows between intervals
───────────────────────────────────
  bootstrap_candidate   – bootstrap names not currently in any Active list
  forgotten_created     – created names dropped from the pipeline (still live
                          in MongoDB; eligible for re-activation)
  active1 … active4     – aging pipeline lists, each entry is
                          {"name": str, "origin": "bootstrap"|"created"}
  pool_expansion        – names not yet used from the expansion pool
  deletion_pool         – remaining names from the retirement sublist

Promotion fractions (configured via sim_params.yaml)
─────────────────────────────────────────────────────
  Active1 → Active2 : ignore 33 %, promote 62 %, delete 5 % of created in promo
  Active2 → Active3 : ignore 70 %, promote 25 %, delete 5 % of created in promo
  Active3 → Active4 : ignore 90 %, promote  5 %, delete 5 % of created in promo
  Active4 → pool    : return all (no deletion)

The 5 % deletion applies ONLY to created-origin items in the promote group.
Name-generation uses a mktemp-style pattern: each 'X' becomes one random
character from [a-z0-9], so the pattern itself fixes the suffix length.
"""
from __future__ import annotations

import json
import os
import random
import string
from typing import Dict, List, Set, Tuple


# ── name generation ───────────────────────────────────────────────────────────

_PATTERN_CHARS = string.ascii_lowercase + string.digits   # [a-z0-9]


def _expand_pattern(pattern: str) -> str:
    """
    Replace every uppercase 'X' in *pattern* with a random character from
    [a-z0-9], mirroring the mktemp(1) convention.

    Example
    -------
    >>> _expand_pattern("lt_XXXXXXXX")
    'lt_3a7fb2c9'
    """
    return "".join(
        random.choice(_PATTERN_CHARS) if ch == "X" else ch
        for ch in pattern
    )


def _gen_expansion_names(
    existing: Set[str],
    count: int,
    pattern: str,
) -> List[str]:
    """
    Generate *count* unique names not in *existing* using mktemp-style *pattern*.

    Raises ValueError if the pattern lacks enough X's to satisfy headroom,
    or RuntimeError if the generation loop stalls (should never occur with
    adequate headroom).
    """
    n_x = pattern.count("X")
    capacity = len(_PATTERN_CHARS) ** n_x
    if capacity < count * 4:
        raise ValueError(
            f"Pattern '{pattern}' has {n_x} X-substitution(s), yielding at most "
            f"{capacity:,} unique values — insufficient headroom for {count:,} names. "
            f"Add more X characters to the pattern."
        )

    names: List[str] = []
    seen = set(existing)
    max_attempts = count * 20

    for _ in range(max_attempts):
        if len(names) >= count:
            break
        candidate = _expand_pattern(pattern)
        if candidate not in seen:
            seen.add(candidate)
            names.append(candidate)
    else:
        raise RuntimeError(
            f"Could not generate {count} unique names after {max_attempts} attempts "
            f"with pattern '{pattern}'. Add more X characters."
        )

    return names


# ── helpers ───────────────────────────────────────────────────────────────────

def _gi(mu: float, sigma: float, lo: int, hi: int) -> int:
    """Sample a clipped Gaussian integer."""
    return max(lo, min(hi, int(round(random.gauss(mu, sigma)))))


def _process_stage(
    entries: List[Dict],
    ignore_frac: float,
    delete_frac_created: float,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Split *entries* into (to_promote, to_delete, to_ignore).

    *ignore_frac* of the list is ignored.
    Of the surviving promote group, *delete_frac_created* of created-origin
    items are marked for permanent deletion instead of promotion.

    Returns
    -------
    to_promote : entries that move to the next active stage
    to_delete  : created-origin entries to be queried then dropped permanently
    to_ignore  : entries that fall out of the pipeline (returned/forgotten)
    """
    n = len(entries)
    if n == 0:
        return [], [], []

    shuffled = random.sample(entries, n)
    n_ignore  = round(n * ignore_frac)
    to_ignore  = shuffled[:n_ignore]
    promo_grp  = shuffled[n_ignore:]

    created_promo = [e for e in promo_grp if e["origin"] == "created"]
    n_delete = int(len(created_promo) * delete_frac_created)
    to_delete: List[Dict] = []
    if n_delete > 0:
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

def preroll(
    names_bootstrap: List[str],
    plan_dir: str,
    params: Dict,
    seed: int | None = None,
) -> None:
    """
    Generate all interval plan files and write them to *plan_dir*.

    Parameters
    ----------
    names_bootstrap : the 1 500 (or however many) bootstrap collection names
    plan_dir        : output directory for plan JSON files
    params          : dict loaded from sim_params.yaml (merged with CLI overrides)
    seed            : optional RNG seed for reproducibility
    """
    if seed is not None:
        random.seed(seed)

    os.makedirs(plan_dir, exist_ok=True)

    # ── unpack parameters ──────────────────────────────────────────────
    n_intervals          = params["n_intervals"]
    n_expansion          = params["n_expansion"]
    n_deletion           = params["n_deletion"]
    name_pattern         = params["name_pattern"]
    delete_frac_created  = params["delete_frac_created"]
    stage_ignore_frac    = params["stage_ignore_frac"]

    create_mu    = params["target_create_mu"]
    create_sigma = params["target_create_sigma"]
    create_min   = params["target_create_min"]
    create_max   = params["target_create_max"]

    bs_mu    = params["bs_activate_mu"]
    bs_sigma = params["bs_activate_sigma"]
    bs_min   = params["bs_activate_min"]
    bs_max   = params["bs_activate_max"]

    react_mu    = params["reactivate_mu"]
    react_sigma = params["reactivate_sigma"]
    react_min   = params["reactivate_min"]
    react_max   = params["reactivate_max"]

    del_mu    = params["del_sublist_mu"]
    del_sigma = params["del_sublist_sigma"]
    del_min   = params["del_sublist_min"]
    del_max   = params["del_sublist_max"]

    # ── generate expansion names ───────────────────────────────────────
    print(f"  Generating {n_expansion:,} expansion names "
          f"(pattern: '{name_pattern}') …")
    names_expansion = _gen_expansion_names(
        set(names_bootstrap), n_expansion, name_pattern
    )
    with open(os.path.join(plan_dir, "names_expansion.json"), "w") as fh:
        json.dump(names_expansion, fh)

    # ── retirement sublist from bootstrap ──────────────────────────────
    n_del_sub = min(n_deletion, len(names_bootstrap))
    deletion_sublist = random.sample(names_bootstrap, n_del_sub)
    with open(os.path.join(plan_dir, "deletion_sublist.json"), "w") as fh:
        json.dump(deletion_sublist, fh)

    # ── initial state ──────────────────────────────────────────────────
    bootstrap_candidate: Set[str]  = set(names_bootstrap)
    forgotten_created:   List[str] = []

    active1: List[Dict] = []
    active2: List[Dict] = []
    active3: List[Dict] = []
    active4: List[Dict] = []

    pool_expansion: List[str] = list(names_expansion)
    deletion_pool:  List[str] = list(deletion_sublist)

    # ── pre-distribute create counts so the pool isn't over-drawn ─────
    create_counts = [
        _gi(create_mu, create_sigma, create_min, create_max)
        for _ in range(n_intervals)
    ]
    total_creates = sum(create_counts)
    if total_creates > len(pool_expansion):
        factor = len(pool_expansion) / total_creates
        create_counts = [
            max(create_min, floor(c * factor))
            for c in create_counts
        ]
        total_creates = sum(create_counts)
        while total_creates > len(pool_expansion):
            idx = max(range(n_intervals), key=lambda i: create_counts[i])
            if create_counts[idx] > create_min:
                create_counts[idx] -= 1
                total_creates -= 1
            else:
                break

    # ── pre-distribute deletion counts to exactly exhaust deletion_pool ─
    if not (del_min * n_intervals <= n_del_sub <= del_max * n_intervals):
        raise ValueError(
            f"n_deletion={n_del_sub} cannot be distributed across {n_intervals} intervals "
            f"within [{del_min}, {del_max}] per interval "
            f"(feasible range: [{del_min * n_intervals}, {del_max * n_intervals}])"
        )
    del_counts = [
        _gi(del_mu, del_sigma, del_min, del_max)
        for _ in range(n_intervals)
    ]
    gap = sum(del_counts) - n_del_sub
    while gap != 0:
        idx = random.randrange(n_intervals)
        old = del_counts[idx]
        new = random.randint(del_min, del_max)
        if gap > 0 and new < old:
            if new < old - gap:      # would overshoot → clamp
                del_counts[idx] = old - gap
                gap = 0
            else:                     # partial reduction → accept
                del_counts[idx] = new
                gap -= old - new
        elif gap < 0 and new > old:
            if new > old - gap:      # would overshoot → clamp
                del_counts[idx] = old - gap
                gap = 0
            else:                     # partial increase → accept
                del_counts[idx] = new
                gap -= old - new

    # ── simulate all intervals ─────────────────────────────────────────
    header = (f"  {'Intv':>4}  {'New':>4}  {'BSAct':>5}  {'React':>5}  "
              f"{'Del':>5}  {'A1':>5}  {'A2':>5}  {'A3':>5}  {'A4':>5}")
    print(f"\n{header}")

    for ix in range(n_intervals):
        interval = ix + 1
        plan: Dict = {"interval": interval}

        ign1 = stage_ignore_frac.get("active1", 0.33)
        ign2 = stage_ignore_frac.get("active2", 0.70)
        ign3 = stage_ignore_frac.get("active3", 0.90)

        # ── 1. Active4 → return to pools ──────────────────────────────
        a4_ret_bs = [e["name"] for e in active4 if e["origin"] == "bootstrap"]
        a4_ret_cr = [e["name"] for e in active4 if e["origin"] == "created"]
        plan["active4_return_bootstrap"] = a4_ret_bs
        plan["active4_return_created"]   = a4_ret_cr
        for name in a4_ret_bs:
            bootstrap_candidate.add(name)
        forgotten_created.extend(a4_ret_cr)
        active4 = []

        # ── 2. Active3 → Active4 ──────────────────────────────────────
        a3_promo, a3_del, a3_ign = _process_stage(active3, ign3, delete_frac_created)
        plan["active3_to_active4"] = a3_promo
        plan["active3_to_delete"]  = a3_del
        plan["active3_to_ignore"]  = a3_ign
        _return_ignored(a3_ign, bootstrap_candidate, forgotten_created)
        active4 = a3_promo
        active3 = []

        # ── 3. Active2 → Active3 ──────────────────────────────────────
        a2_promo, a2_del, a2_ign = _process_stage(active2, ign2, delete_frac_created)
        plan["active2_to_active3"] = a2_promo
        plan["active2_to_delete"]  = a2_del
        plan["active2_to_ignore"]  = a2_ign
        _return_ignored(a2_ign, bootstrap_candidate, forgotten_created)
        active3 = a2_promo
        active2 = []

        # ── 4. Active1 → Active2 ──────────────────────────────────────
        a1_promo, a1_del, a1_ign = _process_stage(active1, ign1, delete_frac_created)
        plan["active1_to_active2"] = a1_promo
        plan["active1_to_delete"]  = a1_del
        plan["active1_to_ignore"]  = a1_ign
        _return_ignored(a1_ign, bootstrap_candidate, forgotten_created)
        active2 = a1_promo
        active1 = []

        # ── 5a. New creates from expansion pool ───────────────────────
        n_create    = min(create_counts[ix], len(pool_expansion))
        new_creates = pool_expansion[:n_create]
        pool_expansion = pool_expansion[n_create:]
        plan["new_creates"] = new_creates

        # ── 5b. Bootstrap activations ─────────────────────────────────
        n_bs = min(
            _gi(bs_mu, bs_sigma, bs_min, bs_max),
            len(bootstrap_candidate),
        )
        bs_activate = (
            random.sample(sorted(bootstrap_candidate), n_bs)
            if n_bs > 0 else []
        )
        for name in bs_activate:
            bootstrap_candidate.discard(name)
        plan["bootstrap_activate"] = bs_activate

        # ── 5c. Forgotten re-activations ──────────────────────────────
        if interval > 1 and forgotten_created:
            n_react = min(
                _gi(react_mu, react_sigma, react_min, react_max),
                len(forgotten_created),
            )
            react = random.sample(forgotten_created, n_react) if n_react > 0 else []
            for name in react:
                forgotten_created.remove(name)
        else:
            react = []
        plan["forgotten_reactivate"] = react

        # ── 5d. Build new Active1 ─────────────────────────────────────
        active1 = (
            [{"name": n, "origin": "created"}   for n in new_creates]
          + [{"name": n, "origin": "bootstrap"} for n in bs_activate]
          + [{"name": n, "origin": "created"}   for n in react]
        )

        # ── 6. Retirement-sublist deletions ───────────────────────────
        n_del  = min(del_counts[ix], len(deletion_pool))
        del_sub = random.sample(deletion_pool, n_del) if n_del > 0 else []
        for name in del_sub:
            deletion_pool.remove(name)
        plan["delete_sublist"] = del_sub

        # purge deleted names from every pool they may currently occupy;
        # record each eviction so the plan accurately reflects all disposition
        # changes within the interval for observability validation
        del_set = set(del_sub)
        bootstrap_candidate -= del_set

        def _evict(stage: List[Dict]) -> Tuple[List[Dict], List[str]]:
            kept    = [e for e in stage if e["name"] not in del_set]
            evicted = [e["name"] for e in stage if e["name"] in del_set]
            return kept, evicted

        active1, evict1 = _evict(active1)
        active2, evict2 = _evict(active2)
        active3, evict3 = _evict(active3)
        active4, evict4 = _evict(active4)
        plan["delete_evict_active1"] = evict1
        plan["delete_evict_active2"] = evict2
        plan["delete_evict_active3"] = evict3
        plan["delete_evict_active4"] = evict4

        # ── stats snapshot ────────────────────────────────────────────
        plan["_stats"] = {
            "active1_size_after":            len(active1),
            "active2_size_after":            len(active2),
            "active3_size_after":            len(active3),
            "active4_size_after":            len(active4),
            "bootstrap_candidate_remaining": len(bootstrap_candidate),
            "forgotten_created_remaining":   len(forgotten_created),
            "pool_expansion_remaining":      len(pool_expansion),
            "deletion_pool_remaining":       len(deletion_pool),
        }

        path = os.path.join(plan_dir, f"interval_{interval:02d}.json")
        with open(path, "w") as fh:
            json.dump(plan, fh, indent=2)

        s = plan["_stats"]
        print(f"  {interval:>4}  {n_create:>4}  {len(bs_activate):>5}  "
              f"{len(react):>5}  {n_del:>5}  "
              f"{s['active1_size_after']:>5}  {s['active2_size_after']:>5}  "
              f"{s['active3_size_after']:>5}  {s['active4_size_after']:>5}")

    # ── overall metadata ──────────────────────────────────────────────
    meta = {
        "n_intervals":                   n_intervals,
        "seed":                          seed,
        "name_pattern":                  name_pattern,
        "n_expansion_generated":         len(names_expansion),
        "n_expansion_consumed":          len(names_expansion) - len(pool_expansion),
        "n_expansion_remaining":         len(pool_expansion),
        "n_deletion_sublist":            len(deletion_sublist),
        "n_deletion_sublist_remaining":  len(deletion_pool),
        "final_bootstrap_candidate":     len(bootstrap_candidate),
        "final_forgotten_created":       len(forgotten_created),
    }
    with open(os.path.join(plan_dir, "preroll_meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"\n  Pre-roll complete – {n_intervals} plans written to: {plan_dir}")
    print(f"  Expansion pool used: {meta['n_expansion_consumed']:,} / "
          f"{meta['n_expansion_generated']:,}  "
          f"({meta['n_expansion_remaining']:,} remaining as padding)")
