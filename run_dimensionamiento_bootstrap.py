from __future__ import annotations

import argparse
import json
import os

from web_comparativas.dimensionamiento.ingestion import bootstrap_dimensionamiento


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap one-off de Dimensionamiento hacia PostgreSQL.")
    parser.add_argument("--source-url", dest="source_url", default=None, help="URL del CSV.")
    parser.add_argument("--csv-path", dest="csv_path", default=None, help="Ruta local del CSV.")
    parser.add_argument("--chunk-size", dest="chunk_size", type=int, default=10000)
    parser.add_argument("--mode", dest="mode", choices=["replace", "upsert"], default="replace")
    parser.add_argument("--force", dest="force", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = bootstrap_dimensionamiento(
        csv_path=args.csv_path,
        source_url=args.source_url or os.getenv("DIMENSIONAMIENTO_CSV_URL"),
        chunk_size=args.chunk_size,
        mode=args.mode,
        force=args.force,
        require_postgres=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
