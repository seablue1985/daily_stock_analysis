# -*- coding: utf-8 -*-
"""
===================================
ThsMarketFetcher - 同花顺本地行情桥接
===================================

通过外部 Python 环境桥接 `thsdata/thsdk`，避免强依赖当前运行环境必须安装 THS SDK。

默认探测顺序：
1. THS_MARKET_PYTHON
2. AlphaCouncil/.venv/bin/python
3. 当前解释器（若已安装 thsdata/thsdk）
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource, safe_float, safe_int

logger = logging.getLogger(__name__)


class ThsMarketFetcher(BaseFetcher):
    name = "ThsMarketFetcher"
    priority = int(os.getenv("THS_MARKET_PRIORITY", "1"))

    def __init__(self, python_bin: Optional[str] = None):
        self._bridge_path = Path(__file__).with_name("ths_market_bridge.py")
        self._python_bin = self._resolve_python_bin(python_bin)

    @staticmethod
    def _resolve_python_bin(python_bin: Optional[str]) -> Optional[str]:
        candidates = [
            python_bin,
            os.getenv("THS_MARKET_PYTHON", "").strip(),
            "/Users/ling/.openclaw/workspace-quant/AlphaCouncil/.venv/bin/python",
            sys.executable,
        ]
        for candidate in candidates:
            if not candidate:
                continue
            path = Path(candidate).expanduser()
            if path.exists():
                return str(path)
        return None

    @staticmethod
    def _normalize_ths_code(stock_code: str) -> str:
        code = stock_code.strip().upper()
        if code.startswith(("USHA", "USZA")) and len(code) == 10:
            return code
        if not code.isdigit() or len(code) != 6:
            raise DataFetchError(f"THS 行情桥接仅支持 A 股 6 位代码，收到: {stock_code}")
        if code.startswith(("60", "68", "51", "56", "58", "11")):
            return f"USHA{code}"
        return f"USZA{code}"

    @staticmethod
    def _build_env() -> dict[str, str]:
        env = os.environ.copy()
        remap = {
            "THS_MARKET_USERNAME": "THS_USERNAME",
            "THS_MARKET_PASSWORD": "THS_PASSWORD",
            "THS_MARKET_MAC": "THS_MAC",
        }
        for source_key, target_key in remap.items():
            source_val = env.get(source_key, "").strip()
            if source_val and not env.get(target_key):
                env[target_key] = source_val
        return env

    def _run_bridge(self, action: str, **kwargs: Any) -> pd.DataFrame:
        if not self._python_bin:
            raise DataFetchError("未找到可用的 THS Python 环境，请设置 THS_MARKET_PYTHON")

        cmd = [self._python_bin, str(self._bridge_path)]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            output_path = Path(tmp.name)
        cmd.extend(["--output", str(output_path), action])
        for key, value in kwargs.items():
            if value is None or value == "":
                continue
            cmd.extend([f"--{key.replace('_', '-')}", str(value)])

        timeout_sec = int(os.getenv("THS_MARKET_TIMEOUT_SEC", "25"))
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                env=self._build_env(),
                timeout=timeout_sec,
            )
        finally:
            output_text = output_path.read_text(encoding="utf-8").strip() if output_path.exists() else ""
            output_path.unlink(missing_ok=True)

        if result.returncode != 0 and not output_text:
            err_text = result.stderr.strip() or "THS bridge failed"
            raise DataFetchError(err_text)

        try:
            payload = json.loads(output_text)
        except json.JSONDecodeError as exc:
            raise DataFetchError(f"THS bridge 返回非 JSON 内容: {exc}") from exc

        if not payload.get("ok"):
            raise DataFetchError(payload.get("error", "THS bridge returned failure"))

        return pd.DataFrame(payload.get("data") or [])

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ths_code = self._normalize_ths_code(stock_code)
        start = datetime.strptime(start_date, "%Y-%m-%d").date().isoformat()
        end = datetime.strptime(end_date, "%Y-%m-%d").date().isoformat()
        period = os.getenv("THS_MARKET_PERIOD", "day")
        adjust = os.getenv("THS_MARKET_ADJUST", "")
        return self._run_bridge(
            "security_bars",
            code=ths_code,
            start=start,
            end=end,
            adjust=adjust,
            period=period,
        )

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=STANDARD_COLUMNS)

        out = df.copy()
        out["time"] = pd.to_datetime(out.get("time"), errors="coerce")
        out = out.dropna(subset=["time", "close"]).sort_values("time").reset_index(drop=True)
        out = out.rename(columns={"time": "date", "turnover": "amount"})

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col not in out.columns:
                out[col] = pd.NA
            out[col] = pd.to_numeric(out[col], errors="coerce")

        out["pct_chg"] = out["close"].pct_change() * 100.0
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        return out[STANDARD_COLUMNS]

    def ths_concept_blocks(self) -> pd.DataFrame:
        return self._run_bridge("ths_concept_block")

    def ths_industry_blocks(self) -> pd.DataFrame:
        return self._run_bridge("ths_industry_block")

    def ths_block_components(self, block_code: str) -> pd.DataFrame:
        return self._run_bridge("block_components", block_code=block_code)

    def get_intraday_snapshot(self, stock_code: str, *, date: str = "") -> pd.DataFrame:
        ths_code = self._normalize_ths_code(stock_code)
        snap = self._run_bridge("min_snapshot", code=ths_code, date=date or None)
        if snap.empty:
            return pd.DataFrame()

        out = snap.copy()
        out["时间"] = pd.to_numeric(out.get("时间"), errors="coerce")
        out["价格"] = pd.to_numeric(out.get("价格"), errors="coerce")
        out["成交量"] = pd.to_numeric(out.get("成交量"), errors="coerce")
        out["总金额"] = pd.to_numeric(out.get("总金额"), errors="coerce")
        out = out.dropna(subset=["时间", "价格"]).sort_values("时间").reset_index(drop=True)
        if out.empty:
            return pd.DataFrame()
        return out

    def get_intraday_ma_support(
        self,
        stock_code: str,
        *,
        window: int,
        min_bias_pct: float = 0.0,
        expected_date_tag: str = "",
    ) -> dict[str, Any]:
        if window <= 0:
            return {}

        snap = self.get_intraday_snapshot(stock_code, date=expected_date_tag)
        if snap.empty:
            return {}

        latest_ts = safe_int(snap.iloc[-1].get("时间"))
        if latest_ts is None:
            return {}

        source_date_tag = datetime.fromtimestamp(latest_ts).strftime("%Y%m%d")
        normalized_expected = str(expected_date_tag or "").replace("-", "").strip()
        if normalized_expected and normalized_expected != source_date_tag:
            return {}

        closes = pd.to_numeric(snap.get("价格"), errors="coerce").dropna().tolist()
        if not closes:
            return {}

        sample = closes[-max(1, int(window)) :]
        ma_value = sum(sample) / len(sample) if sample else 0.0
        last_close = float(closes[-1]) if closes else 0.0
        bias_pct = ((last_close / ma_value) - 1.0) * 100.0 if ma_value > 0 else None
        supported = bool(bias_pct is not None and bias_pct >= float(min_bias_pct))
        return {
            "intraday_ma_window": int(max(1, int(window))),
            "intraday_ma": round(ma_value, 4) if ma_value > 0 else None,
            "intraday_ma_last": round(last_close, 4) if last_close > 0 else None,
            "intraday_ma_bias_pct": round(float(bias_pct), 4) if bias_pct is not None else None,
            "intraday_ma_supported": supported,
            "intraday_ma_source": RealtimeSource.THS_MARKET.value,
            "intraday_ma_source_date_tag": source_date_tag,
        }

    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        ths_code = self._normalize_ths_code(stock_code)
        out = self.get_intraday_snapshot(stock_code)
        if out.empty:
            return None

        latest = out.iloc[-1]
        trade_date = datetime.fromtimestamp(int(latest["时间"])).date()
        bars = self._run_bridge(
            "security_bars",
            code=ths_code,
            start=(trade_date - timedelta(days=14)).isoformat(),
            end=trade_date.isoformat(),
            adjust=os.getenv("THS_MARKET_ADJUST", ""),
            period=os.getenv("THS_MARKET_PERIOD", "day"),
        )

        pre_close = None
        if not bars.empty and "time" in bars.columns and "close" in bars.columns:
            hist = bars.copy()
            hist["time"] = pd.to_datetime(hist["time"], errors="coerce")
            hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
            hist = hist.dropna(subset=["time", "close"]).sort_values("time")
            previous = hist[hist["time"].dt.date < trade_date]
            if not previous.empty:
                pre_close = safe_float(previous.iloc[-1]["close"])
            elif len(hist) >= 2:
                pre_close = safe_float(hist.iloc[-2]["close"])

        price = safe_float(latest.get("价格"))
        change_amount = price - pre_close if price is not None and pre_close is not None else None
        change_pct = (
            (price / pre_close - 1.0) * 100.0
            if price is not None and pre_close not in (None, 0)
            else None
        )

        return UnifiedRealtimeQuote(
            code=stock_code,
            source=RealtimeSource.THS_MARKET,
            price=price,
            change_pct=change_pct,
            change_amount=change_amount,
            volume=safe_int(out["成交量"].sum()),
            amount=safe_float(out["总金额"].sum()),
            open_price=safe_float(out.iloc[0].get("价格")),
            high=safe_float(out["价格"].max()),
            low=safe_float(out["价格"].min()),
            pre_close=pre_close,
        )
