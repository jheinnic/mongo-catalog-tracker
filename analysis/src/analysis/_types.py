import pathlib
import re
import argparse
from typing import Literal, TypedDict

WarehouseFormat = Literal["orc", "parquet"]

class LabelByActivityCommand(TypedDict):
    warehouse_root: str

class LabelInactivePolicyCommand(TypedDict):
    warehouse_root: str
    n_inactivity:   int

class QueryActivitySankeyCommand(TypedDict):
    warehouse_root: str

class DeriveRangesCommand(TypedDict):
    warehouse_root: str

class LoadDataCommand(TypedDict):
    data_lake_root: str
    warehouse_root: str
    label: str

class RedoAggCommand(TypedDict):
    warehouse_root: str

class RedoCollectionAggCommand(TypedDict):
    warehouse_root: str

class RedoIndexAggCommand(TypedDict):
    warehouse_root: str


_KNOWN_SCHEMES = re.compile(r"^(s3a|s3|hdfs|gs|abfs|file)://")

def validate_warehouse_path(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("warehouse_root must not be empty")
    if not (value.startswith("/") or _KNOWN_SCHEMES.match(value)):
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an absolute path or a recognised scheme "
            f"(s3a://, hdfs://, gs://, etc.)"
        )
    return value

def resolve_format(warehouse_root: str, explicit: str | None) -> WarehouseFormat:
    if explicit:
        return explicit  # type: ignore[return-value]
    parts = set(pathlib.PurePosixPath(warehouse_root).parts)
    has_orc     = "orc"     in parts
    has_parquet = "parquet" in parts
    if has_orc ^ has_parquet:
        return "orc" if has_orc else "parquet"
    raise SystemExit(
        "Cannot infer warehouse format from path — "
        "supply --warehouse-format orc|parquet"
    )
