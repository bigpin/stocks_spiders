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


_STOCK_HEADER_RE = re.compile(r"^股票\s+(?P<stock_name>.+?)\s*\((?P<stock_code>[^)]+)\)\s+股票信号分析结果\s*$")
_OVERALL_RATE_RE = re.compile(r"^总体成功率:\s*(?P<rate>[\d.]+)%\s*$")
_TOTAL_SIGNALS_RE = re.compile(r"^总信号数:\s*(?P<count>\d+)\s*$")
_TOTAL_SUCCESS_RE = re.compile(r"^总成功数:\s*(?P<count>\d+)\s*$")
_TRADE_HEAT_RE = re.compile(
    r"^最近交易热度评分:\s*(?P<score>[\d.]+)\s*/\s*(?P<max>[\d.]+)\s*$"
)
_STOP_LOSS_RE = re.compile(r"^止损位:\s*(?P<sl>[\d.]+)\s*$")
_SUGGESTED_EXIT_RE = re.compile(r"^建议退出:\s*(?P<se>.+)$")

# 宽松匹配事件行：以"股票:"开头，后续字段按 key: value 模式解析
_EVENT_LINE_PREFIX_RE = re.compile(r"^股票:\s*(?P<stock_name>.+?)\((?P<stock_code>[^)]+)\)")


def _normalize_separators(line: str) -> str:
    """统一标点符号：全角逗号→半角，连续空格→单空格"""
    line = line.replace("，", ",").replace("：", ":")
    # 合并连续空格（保留单个空格）
    line = re.sub(r"\s+", " ", line)
    return line.strip()


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


def _parse_event_line(line: str) -> Optional[Dict[str, Any]]:
    """
    灵活解析事件行，兼容全角/半角标点、多空格、字段缺失等场景。
    解析策略：先匹配股票前缀，再用正则逐个提取 key: value 字段。
    """
    normalized = _normalize_separators(line)
    m = _EVENT_LINE_PREFIX_RE.match(normalized)
    if not m:
        return None

    stock_name = m.group("stock_name").strip()
    stock_code = m.group("stock_code").strip()

    # 定义必需字段和可选字段的提取模式
    field_patterns = {
        "signal_date": re.compile(r"日期:\s*(\d{4}-\d{2}-\d{2})"),
        "signal_type": re.compile(r"信号类型:\s*([^,]+)"),
        "signal_label": re.compile(r"信号:\s*([^,]+)"),
        "signal_success_rate": re.compile(r"信号胜率:\s*([\d.]+)%"),
        "signal_total": re.compile(r"历史出现:\s*(\d+)次"),
        "overall_success_rate": re.compile(r"整体胜率:\s*([\d.]+)%"),
        "close": re.compile(r"收盘价:\s*([\d.]+)"),
    }

    result: Dict[str, Any] = {
        "stock_code": stock_code,
        "stock_name": stock_name,
    }

    # 提取已知字段
    for key, pat in field_patterns.items():
        fm = pat.search(normalized)
        if fm:
            val = fm.group(1)
            if key in ("signal_success_rate", "overall_success_rate", "close"):
                result[key] = float(val)
            elif key == "signal_total":
                result[key] = int(val)
            else:
                result[key] = val.strip()

    # 验证必需字段存在
    required = ("signal_date", "signal_type", "signal_label", "signal_success_rate",
                "signal_total", "overall_success_rate")
    if not all(k in result for k in required):
        return None

    # 解析尾部附加指标（从收盘价之后开始）
    close_match = field_patterns["close"].search(normalized)
    if close_match:
        rest_start = close_match.end()
        rest_str = normalized[rest_start:].lstrip(", ")
    else:
        rest_str = ""

    result["metrics"] = parse_metrics(rest_str)
    # close 为可选字段
    if "close" not in result:
        result["close"] = None

    return result


@dataclass
class ParsedStockSection:
    stock_code: str
    stock_name: str
    overall_success_rate: Optional[float] = None
    total_signal_count: Optional[int] = None
    total_success_count: Optional[int] = None
    # 与 stock_kline 输出「最近交易热度评分: xx/100」对应
    trade_heat_score: Optional[float] = None
    trade_heat_max: Optional[float] = None
    # 个股级止损 / 退出建议（每只股票一条，紧跟热度评分之后）
    stop_loss: Optional[float] = None
    suggested_exit: Optional[str] = None
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

        m = _TRADE_HEAT_RE.match(line)
        if m:
            current.trade_heat_score = float(m.group("score"))
            current.trade_heat_max = float(m.group("max"))
            continue

        m = _STOP_LOSS_RE.match(line)
        if m:
            current.stop_loss = float(m.group("sl"))
            continue

        m = _SUGGESTED_EXIT_RE.match(line)
        if m:
            current.suggested_exit = m.group("se").strip()
            continue

        ev = _parse_event_line(line)
        if ev:
            current.events.append(ev)
            continue

    return sections


def parse_daily_report_file(path: str) -> Tuple[List[str], List[ParsedStockSection]]:
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    sections = parse_daily_report_lines(lines)
    return lines, sections

