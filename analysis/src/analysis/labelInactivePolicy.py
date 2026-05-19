import argparse
from analysis._types import LabelInactivePolicyCommand, validate_warehouse_path, resolve_format


def main() -> None:
    parser = argparse.ArgumentParser(description="Label collections by inactivity policy")
    parser.add_argument("--warehouseRoot", type=validate_warehouse_path,
                        help="Root path of the data lake warehouse")
    parser.add_argument("--nInactivity", type=int, nargs="?", default=14,
                        help="Lookback window in samples for inactivity detection (default: 14)")
    parser.add_argument("--warehouseFormat", choices=["orc", "parquet"],
                        help="Storage format (inferred from path if omitted)")
    args, _ = parser.parse_known_args()

    command: LabelInactivePolicyCommand = {
        "warehouse_root": args.warehouseRoot,
        "n_inactivity":   args.nInactivity,
    }
    fmt = resolve_format(args.warehouseRoot, args.warehouseFormat)

    if fmt == "orc":
        from analysis.orc.labelInactivePolicy import main
    else:
        from analysis.parquet.labelInactivePolicy import main

    main(command)
