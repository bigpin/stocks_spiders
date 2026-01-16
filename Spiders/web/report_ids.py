#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import re
from dataclasses import dataclass


_FILENAME_RE = re.compile(r"kdj_signals_(\d{4})(\d{2})(\d{2})\.txt$")


class ReportIdError(ValueError):
    pass


@dataclass(frozen=True)
class ReportKey:
    report_date: str  # YYYY-MM-DD
    stock_code: str   # e.g. sh601231

    @property
    def report_id(self) -> str:
        return make_report_id(self.report_date, self.stock_code)


def report_date_from_filename(path: str) -> str:
    base = os.path.basename(path)
    m = _FILENAME_RE.match(base)
    if not m:
        raise ReportIdError(f"Bad report filename (expect kdj_signals_YYYYMMDD.txt): {base}")
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"


def make_report_id(report_date: str, stock_code: str) -> str:
    return f"report_{report_date}_{stock_code}"


def make_event_id(report_date: str, stock_code: str, signal_date: str, signal_type: str) -> str:
    # Keep _id stable and URL/JS safe: replace spaces just in case.
    st = signal_type.replace(" ", "_")
    return f"event_{report_date}_{stock_code}_{signal_date}_{st}"

