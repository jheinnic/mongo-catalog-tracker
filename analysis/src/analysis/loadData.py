import argparse
from analysis._types import LoadDataCommand, validate_warehouse_path, resolve_format


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw CSV keyhole data into the warehouse")
    parser.add_argument("data_lake_root", type=validate_warehouse_path,
                        help="Root path of the raw data lake (source CSVs)")
    parser.add_argument("warehouse_root", type=validate_warehouse_path,
                        help="Root path of the data lake warehouse (destination)")
    parser.add_argument("label",
                        help="Subdirectory label within data_lake_root (e.g. demo01)")
    parser.add_argument("--warehouse-format", choices=["orc", "parquet"],
                        help="Storage format (inferred from warehouse_root path if omitted)")
    args, _ = parser.parse_known_args()

    command: LoadDataCommand = {
        "data_lake_root": args.data_lake_root,
        "warehouse_root": args.warehouse_root,
        "label":          args.label,
    }
    fmt = resolve_format(args.warehouse_root, args.warehouse_format)

    if fmt == "orc":
        from analysis.orc.loadData import main as _run
    else:
        from analysis.parquet.loadData import main as _run

    _run(command)
