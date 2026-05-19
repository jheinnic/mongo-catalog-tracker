import argparse
from analysis._types import RedoAggCommand, validate_warehouse_path, resolve_format


def main() -> None:
    parser = argparse.ArgumentParser(description="Recompute collection and index ops aggregates")
    parser.add_argument("warehouse_root", type=validate_warehouse_path,
                        help="Root path of the data lake warehouse")
    parser.add_argument("--warehouse-format", choices=["orc", "parquet"],
                        help="Storage format (inferred from path if omitted)")
    args, _ = parser.parse_known_args()

    command: RedoAggCommand = {"warehouse_root": args.warehouse_root}
    fmt = resolve_format(args.warehouse_root, args.warehouse_format)

    if fmt == "orc":
        from analysis.orc.redoAgg import main as _run
    else:
        from analysis.parquet.redoAgg import main as _run

    _run(command)
