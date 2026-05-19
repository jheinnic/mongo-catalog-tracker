import argparse
from analysis._types import LabelInactivePolicyCommand, validate_warehouse_path, resolve_format


def main() -> None:
    parser = argparse.ArgumentParser(description="Label collections by inactivity policy")
    parser.add_argument("warehouse_root", type=validate_warehouse_path,
                        help="Root path of the data lake warehouse")
    parser.add_argument("n_inactivity", type=int, nargs="?", default=14,
                        help="Lookback window in samples for inactivity detection (default: 14)")
    parser.add_argument("--warehouse-format", choices=["orc", "parquet"],
                        help="Storage format (inferred from path if omitted)")
    args, _ = parser.parse_known_args()

    command: LabelInactivePolicyCommand = {
        "warehouse_root": args.warehouse_root,
        "n_inactivity":   args.n_inactivity,
    }
    fmt = resolve_format(args.warehouse_root, args.warehouse_format)

    if fmt == "orc":
        from analysis.orc.labelInactivePolicy import main
    else:
        from analysis.parquet.labelInactivePolicy import main

    main(command)
