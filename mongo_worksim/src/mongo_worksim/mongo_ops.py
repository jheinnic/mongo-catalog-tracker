"""
mongo_ops.py  –  low-level MongoDB collection helpers
"""
from __future__ import annotations

import datetime
import random
import logging

import pymongo
import pymongo.errors

log = logging.getLogger(__name__)

# Tags used as an indexed field so queries reliably hit score_idx
_TAGS = [
    "alpha", "beta", "gamma", "delta", "epsilon",
    "zeta", "eta", "theta", "iota", "kappa",
]


# ── document factory ──────────────────────────────────────────────────────────

def _make_docs(coll_name: str) -> list:
    """Return 5 synthetic documents whose 'score' field is indexed."""
    now = datetime.datetime.utcnow()
    return [
        {
            "label":   f"{coll_name[:20]}_doc{i}",
            "score":   random.randint(1, 1000),
            "tag":     random.choice(_TAGS),
            "created": now,
            "seq":     i,
        }
        for i in range(5)
    ]


# ── collection lifecycle ──────────────────────────────────────────────────────

def create_collection(db, name: str) -> None:
    """
    Create *name* with 5 synthetic documents and an ascending index on 'score'.
    Silently ignores collections that already exist.
    """
    try:
        col = db[name]
        col.insert_many(_make_docs(name))
        col.create_index(
            [("score", pymongo.ASCENDING)],
            name="score_idx",
            background=True,
        )
    except (pymongo.errors.OperationFailure, pymongo.errors.PyMongoError) as exc:
        log.warning("create_collection(%s): %s", name, exc)


def query_collection(db, name: str) -> None:
    """
    Issue three range queries against score_idx, forcing three index scans
    and incrementing the collection's op-counter each time.
    """
    try:
        col = db[name]
        list(col.find({"score": {"$gt":  250}}).limit(10))
        list(col.find({"score": {"$lt":  750}}).limit(10))
        list(col.find({"score": {"$gte": 100, "$lte": 900}}).limit(10))
    except (pymongo.errors.OperationFailure, pymongo.errors.PyMongoError) as exc:
        log.warning("query_collection(%s): %s", name, exc)


def delete_collection(db, name: str) -> None:
    """Drop *name* and all its indexes.  No-ops if the collection is absent."""
    try:
        db[name].drop()
    except (pymongo.errors.OperationFailure, pymongo.errors.PyMongoError) as exc:
        log.warning("delete_collection(%s): %s", name, exc)


def query_then_delete(db, name: str) -> None:
    """Query (raising OpCount) then permanently drop."""
    query_collection(db, name)
    delete_collection(db, name)


def exercise_collection(db, name: str) -> None:
    """Create, index, and immediately query a brand-new collection."""
    create_collection(db, name)
    query_collection(db, name)
