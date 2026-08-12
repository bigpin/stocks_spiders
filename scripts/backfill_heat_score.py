#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 kdj_signals_YYYYMMDD.txt 中的「最近交易热度评分」回填到本地 SQLite 数据库。

用法：
  python scripts/backfill_heat_score.py kdj_signals_*.txt
  python scripts/backfill_heat_score.py kdj_signals_20260320.txt kdj_signals_20260324.txt
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys

# ── 路径设置 ──────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _SCRIPT_DIR)
import sys as _sys, os as _os
_p = _os.path.dirname(_os.path.abspath(__file__))
while _p and _p != _os.path.dirname(_p) and not _os.path.isdir(_os.path.join(_p, 'Spiders')):
    _p = _os.path.dirname(_p)
if _p and _os.path.isdir(_os.path.join(_p, 'Spiders')) and _p not in _sys.path:
    _sys.path.insert(0, _p)
from Spiders.common.log import get_logger
logger = get_logger(__name__)

DB_PATH = os.path.join(_ROOT_DIR, "stock_signals.db")

_FILENAME_DATE_RE = re.compile(r"kdj_signals_(\d{8})\.txt$", re.IGNORECASE)
_SECTION_HEADER_RE = re.compile(
    r"^股票\s+.+?\((?P<code>[^)]+)\)\s+股票信号分析结果\s*$"
)
_HEAT_RE = re.compile(
    r"^最近交易热度评分:\s*(?P<score>[\d.]+)\s*/\s*[\d.]+\s*$"
)


def _parse_heat_scores(file_path: str) -> dict[str, float]:
    """返回 {stock_code: heat_score} 映射。"""
    scores: dict[str, float] = {}
    current_code: str | None = None

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            m = _SECTION_HEADER_RE.match(line.strip())
            if m:
                current_code = m.group("code").strip()
                continue
            if current_code:
                m2 = _HEAT_RE.match(line.strip())
                if m2:
                    scores[current_code] = float(m2.group("score"))
    return scores


def _report_date_from_filename(file_path: str) -> str | None:
    """从文件名提取 YYYY-MM-DD 格式日期。"""
    basename = os.path.basename(file_path)
    m = _FILENAME_DATE_RE.search(basename)
    if not m:
        return None
    raw = m.group(1)  # e.g. 20260324
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"


def backfill(file_path: str, conn: sqlite3.Connection, dry_run: bool = False) -> tuple[int, int]:
    report_date = _report_date_from_filename(file_path)
    if not report_date:
        logger.warning(f"[WARN] 无法从文件名提取日期，跳过: {file_path}")
        return 0, 0

    scores = _parse_heat_scores(file_path)
    if not scores:
        logger.info(f"[INFO] {file_path}: 未找到热度评分，跳过。")
        return 0, 0

    cursor = conn.cursor()
    updated = 0
    skipped = 0

    for stock_code, score in scores.items():
        # 查找该 stock_code + insert_date 的记录
        cursor.execute(
            "SELECT id, trade_heat_score FROM stock_signals WHERE stock_code = ? AND insert_date = ?",
            (stock_code, report_date),
        )
        rows = cursor.fetchall()
        if not rows:
            skipped += 1
            continue
        for row in rows:
            sig_id, existing = row[0], row[1]
            if existing is not None:
                logger.info(f"  [SKIP] {stock_code} @ {report_date}: 已有评分 {existing}，跳过（新值 {score}）")
                skipped += 1
                continue
            if not dry_run:
                cursor.execute(
                    "UPDATE stock_signals SET trade_heat_score = ? WHERE id = ?",
                    (score, sig_id),
                )
            logger.info(f"  {'[DRY]' if dry_run else '[OK] '} {stock_code} @ {report_date}: 评分 → {score}")
            updated += 1

    if not dry_run:
        conn.commit()
    return updated, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description="回填交易热度评分到 stock_signals 表")
    parser.add_argument("files", nargs="+", help="kdj_signals_YYYYMMDD.txt 文件路径")
    parser.add_argument("--db", default=DB_PATH, help=f"SQLite 数据库路径（默认: {DB_PATH}）")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写入数据库")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        logger.error(f"[ERROR] 数据库不存在: {args.db}")
        return 2

    conn = sqlite3.connect(args.db)
    total_updated = 0
    total_skipped = 0

    for fpath in args.files:
        fpath = os.path.abspath(fpath)
        if not os.path.exists(fpath):
            logger.warning(f"[WARN] 文件不存在，跳过: {fpath}")
            continue
        logger.info(f"\n── 处理 {os.path.basename(fpath)} ──")
        u, s = backfill(fpath, conn, dry_run=args.dry_run)
        total_updated += u
        total_skipped += s

    conn.close()
    logger.info(f"\n完成：更新 {total_updated} 条，跳过 {total_skipped} 条。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
