#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse kdj_signals_YYYYMMDD.txt into:
  - stock_summary docs (report header per stock)
  - signal_event docs (report body per event)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class ReportParseError(ValueError):
    pass


_STOCK_HEADER_RE = re.compile(r"^股票\s+(?P<stock_name>.+?)\((?P<stock_code>[^)]+)\)\s+股票信号分析结果\s*$")
_OVERALL_RATE_RE = re.compile(r"^总体成功率:\s*(?P<rate>[\d.]+)%\s*$")
_TOTAL_SIGNALS_RE = re.compile(r"^总信号数:\s*(?P<count>\d+)\s*$")
_TOTAL_SUCCESS_RE = re.compile(r"^总成功数:\s*(?P<count>\d+)\s*$")

_EVENT_LINE_RE = re.compile(
    r"^股票:\s*(?P<stock_name>.+?)\((?P<stock_code>[^)]+)\),\s*"
    r"日期:\s*(?P<signal_date>\d{4}-\d{2}-\d{2}),\s*"
    r"信号类型:\s*(?P<signal_type>[^,]+),\s*"
    r"信号:\s*(?P<signal_label>[^,]+),\s*"
    r"信号胜率:\s*(?P<signal_success_rate>[\d.]+)%,\s*"
    r"\(历史出现:\s*(?P<signal_total>\d+)次\),\s*"
    r"整体胜率:\s*(?P<overall_success_rate>[\d.]+)%,\s*"
    r"收盘价:\s*(?P<close>[\d.]+)"
    r"(?:,\s*(?P<rest>.*))?\s*$"
)


def _maybe_number(s: str) -> Any:
    s = s.strip()
    if not s:
        return s
    if s.upper() == "N/A":
        return None
    try:
        return int(s)
    except Exception:
        pass
    try:
        return float(s)
    except Exception:
        return s


def parse_metrics(rest: str) -> Dict[str, Any]:
    """
    Parse trailing "key: value, key2: value2" into dict.
    Keys may contain parentheses / Chinese.
    """
    metrics: Dict[str, Any] = {}
    if not rest:
        return metrics
    # Split by comma, but keep it simple: report lines use ", " as separator.
    parts = [p.strip() for p in rest.split(",") if p.strip()]
    for p in parts:
        if ":" not in p:
            continue
        k, v = p.split(":", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        metrics[k] = _maybe_number(v)
    return metrics


@dataclass
class ParsedStockSection:
    stock_code: str
    stock_name: str
    overall_success_rate: Optional[float] = None
    total_signal_count: Optional[int] = None
    total_success_count: Optional[int] = None
    events: List[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []


def parse_daily_report_lines(lines: List[str]) -> List[ParsedStockSection]:
    sections: List[ParsedStockSection] = []
    current: Optional[ParsedStockSection] = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        m = _STOCK_HEADER_RE.match(line)
        if m:
            current = ParsedStockSection(
                stock_code=m.group("stock_code").strip(),
                stock_name=m.group("stock_name").strip(),
            )
            sections.append(current)
            continue

        if current is None:
            continue

        m = _OVERALL_RATE_RE.match(line)
        if m:
            current.overall_success_rate = float(m.group("rate"))
            continue

        m = _TOTAL_SIGNALS_RE.match(line)
        if m:
            current.total_signal_count = int(m.group("count"))
            continue

        m = _TOTAL_SUCCESS_RE.match(line)
        if m:
            current.total_success_count = int(m.group("count"))
            continue

        m = _EVENT_LINE_RE.match(line)
        if m:
            rest = m.group("rest") or ""
            ev = {
                "stock_code": m.group("stock_code").strip(),
                "stock_name": m.group("stock_name").strip(),
                "signal_date": m.group("signal_date"),
                "signal_type": m.group("signal_type").strip(),
                "signal_label": m.group("signal_label").strip(),
                "signal_success_rate": float(m.group("signal_success_rate")),
                "signal_total": int(m.group("signal_total")),
                "overall_success_rate": float(m.group("overall_success_rate")),
                "close": float(m.group("close")),
                "metrics": parse_metrics(rest),
            }
            current.events.append(ev)
            continue

    return sections


def parse_daily_report_file(path: str) -> Tuple[List[str], List[ParsedStockSection]]:
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    sections = parse_daily_report_lines(lines)
    return lines, sections

