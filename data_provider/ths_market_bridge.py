#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

import pandas as pd
from thsdata import FuquanNo, KlineDay, Quote


def serialize_frame(df: pd.DataFrame) -> list[dict[str, Any]]:
    frame = df.copy()
    for col in frame.columns:
        if pd.api.types.is_datetime64_any_dtype(frame[col]):
            frame[col] = frame[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    frame = frame.astype(object).where(pd.notna(frame), None)
    return frame.to_dict(orient="records")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge thsdata/thsdk calls into JSON output")
    parser.add_argument("--output", default="")
    subparsers = parser.add_subparsers(dest="action", required=True)

    bars = subparsers.add_parser("security_bars")
    bars.add_argument("--code", required=True)
    bars.add_argument("--start", required=True)
    bars.add_argument("--end", required=True)
    bars.add_argument("--adjust", default=FuquanNo)
    bars.add_argument("--period", default=KlineDay)

    subparsers.add_parser("ths_concept_block")
    subparsers.add_parser("ths_industry_block")

    snap = subparsers.add_parser("min_snapshot")
    snap.add_argument("--code", required=True)
    snap.add_argument("--date", default="")

    components = subparsers.add_parser("block_components")
    components.add_argument("--block-code", required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    quote = Quote()
    try:
        if args.action == "security_bars":
            start = datetime.fromisoformat(args.start)
            end = datetime.fromisoformat(args.end)
            frame = quote.security_bars(args.code, start, end, args.adjust, args.period)
        elif args.action == "ths_concept_block":
            frame = quote.ths_concept_block()
        elif args.action == "ths_industry_block":
            frame = quote.ths_industry_block()
        elif args.action == "min_snapshot":
            frame = pd.DataFrame(quote.ths.min_snapshot(args.code, date=args.date or None).data or [])
        elif args.action == "block_components":
            frame = quote.ths_block_components(args.block_code)
        else:
            raise ValueError(f"Unsupported action: {args.action}")

        payload = json.dumps({"ok": True, "data": serialize_frame(frame)}, ensure_ascii=False)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                fh.write(payload)
        else:
            print(payload)
    except Exception as exc:  # pragma: no cover - runtime bridge
        payload = json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                fh.write(payload)
        else:
            print(payload)
        raise SystemExit(1)
    finally:
        try:
            quote.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
