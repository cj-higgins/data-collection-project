#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finalize_pdfs_offline.py
Offline (local) HTML→PDF renderer for SEC filings previously downloaded as HTML.

Fixes:
- Removed problematic `global` use. All timing/retry values are handled locally.
- Keeps NaN/blank URL cleaning, polite delays, retries, checksum tracking.

Example run (Windows with forward slashes):
  python finalize_pdfs_offline.py tasks_master_finalized.csv \
    --outdir "C:/Users/you/OneDrive/Documents/SEC PDFs" \
    --only-missing --overwrite --debug \
    --ua "TrialDataCollection/1.0 (you@example.com)"
"""

import argparse
import csv
import hashlib
import os
import random
import re
import time
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from playwright.sync_api import sync_playwright

# ------------------------------- Config --------------------------------

DEFAULT_UA = "TrialDataCollection/1.0 (contact@example.com)"
BLOCK_SNIPPET = "Your Request Originates from an Undeclared Automated Tool"

# ------------------------------ Helpers --------------------------------

def _sanitize(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", "", text)
    return re.sub(r"[\\/:*?\"<>|]", "", text)[:150]

def _clean_url(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    return s

def _checksum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _ensure_cols(df: pd.DataFrame, cols: Iterable[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Input CSV is missing columns: {missing}")

def _session(ua: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua, "Accept-Encoding": "gzip, deflate"})
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def _download_html_once(session, url: str, path: Path, ua: str, debug: bool=False) -> bool:
    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()
        text = r.text
        if BLOCK_SNIPPET.lower() in text.lower():
            if debug: print(f"[BLOCKED] {url}")
            return False
        path.write_text(text, encoding="utf-8", errors="ignore")
        if debug: print(f"[DOWNLOADED] {url} -> {path}")
        return True
    except Exception as e:
        if debug: print(f"[ERROR] download {url}: {e}")
        return False

def _download_html_with_retries(session, url: str, path: Path,
                                sleep_min: float, sleep_max: float, max_fetch_retries: int,
                                debug: bool=False) -> bool:
    for attempt in range(1, max_fetch_retries + 2):
        ok = _download_html_once(session, url, path, ua=session.headers.get("User-Agent",""), debug=debug)
        if ok:
            return True
        sleep_s = (sleep_min + random.random() * (sleep_max - sleep_min)) + attempt * 0.6
        if debug: print(f"[RETRY] {url} in {sleep_s:.2f}s (attempt {attempt})")
        time.sleep(sleep_s)
    return False

def _render_local_html(page, html_path: Path, pdf_path: Path, debug: bool=False) -> bool:
    try:
        file_url = html_path.resolve().as_uri()
        page.goto(file_url, wait_until="load", timeout=60_000)
        page.pdf(
            path=str(pdf_path),
            format="A4",
            margin={"top": "1in", "bottom": "1in", "left": "1in", "right": "1in"},
            print_background=False,
            display_header_footer=False,
            landscape=False,
        )
        if debug: print(f"[RENDERED] {pdf_path}")
        return True
    except Exception as e:
        if debug: print(f"[ERROR] render {html_path} -> {pdf_path}: {e}")
        return False

def _determine_filename(row: pd.Series, idx: int) -> str:
    parts = [
        _sanitize(row.get("TaskID")),
        _sanitize(row.get(f"Ticker_{idx}")),
        _sanitize(row.get(f"Form_{idx}")),
        _sanitize(row.get(f"Period_{idx}")),
        _sanitize(row.get(f"FilingDate_{idx}")),
    ]
    if idx == 2:
        parts.append("doc2")
    base = "_".join([p for p in parts if p]) or f"row_{idx}"
    return f"{base}.pdf"

# ------------------------------- Main ----------------------------------

def main():
    ap = argparse.ArgumentParser(description="Render local HTML copies of SEC filings to PDF (offline).")
    ap.add_argument("csv_file", help="Path to tasks CSV")
    ap.add_argument("--outdir", required=True, help="Directory to write PDFs")
    ap.add_argument("--ua", default=DEFAULT_UA, help="User-Agent (include contact per SEC guidance)")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--only-missing", dest="only_missing", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--sleep-min", type=float, default=0.8)
    ap.add_argument("--sleep-max", type=float, default=1.4)
    ap.add_argument("--max-fetch-retries", type=int, default=2)

    args = ap.parse_args()

    sleep_min = args.sleep_min
    sleep_max = max(args.sleep_min, args.sleep_max)
    max_fetch_retries = max(0, args.max_fetch_retries)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    html_cache = Path("html_cache"); html_cache.mkdir(exist_ok=True)

    df = pd.read_csv(args.csv_file)
    _ensure_cols(df, ["TaskID", "OpenAsHTMLURL_1", "OpenAsHTMLURL_2", "Ticker_1", "Form_1", "FilingDate_1"])
    for col in ("PDF_filename_1", "PDF_checksum_1", "PDF_filename_2", "PDF_checksum_2"):
        if col not in df.columns:
            df[col] = ""

    s = _session(args.ua)

    failed_exists = os.path.exists("failed_rows.csv")
    with open("failed_rows.csv", "a", newline="", encoding="utf-8") as failed_file:
        failed_writer = csv.DictWriter(failed_file, fieldnames=["TaskID", "URL", "Reason"])
        if not failed_exists:
            failed_writer.writeheader()

        from tqdm import tqdm
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(user_agent=args.ua, locale="en-US")
            page = context.new_page()

            for idx_row, row in tqdm(df.iterrows(), total=len(df)):
                for doc_idx in (1, 2):
                    url = _clean_url(row.get(f"OpenAsHTMLURL_{doc_idx}"))
                    if not url:
                        if args.debug: print(f"[SKIP] row {idx_row} doc{doc_idx}: no URL")
                        continue

                    if args.only_missing and str(row.get(f"PDF_filename_{doc_idx}") or "").strip():
                        if args.debug: print(f"[SKIP] row {idx_row} doc{doc_idx}: already has PDF_filename")
                        continue

                    filename = _determine_filename(row, doc_idx)
                    pdf_path = outdir / filename
                    html_name = filename.replace(".pdf", ".html")
                    html_path = html_cache / html_name

                    need_download = (not html_path.exists()) or args.overwrite
                    if need_download:
                        ok = _download_html_with_retries(s, url, html_path,
                                                         sleep_min, sleep_max, max_fetch_retries,
                                                         debug=args.debug)
                        if not ok:
                            failed_writer.writerow({"TaskID": row.get("TaskID"), "URL": url, "Reason": "download_failed_or_blocked"})
                            if args.debug: print(f"[FAIL] download {url}")
                            continue
                        time.sleep(sleep_min + random.random() * (sleep_max - sleep_min))

                    try:
                        size = html_path.stat().st_size
                    except Exception:
                        size = 0
                    if size < 1024:
                        failed_writer.writerow({"TaskID": row.get("TaskID"), "URL": url, "Reason": f"html_too_small({size})"})
                        if args.debug: print(f"[FAIL] tiny html ({size} bytes): {html_path}")
                        continue

                    ok = _render_local_html(page, html_path, pdf_path, debug=args.debug)
                    if ok and pdf_path.exists():
                        df.at[idx_row, f"PDF_filename_{doc_idx}"] = filename
                        df.at[idx_row, f"PDF_checksum_{doc_idx}"] = _checksum(pdf_path)
                    else:
                        failed_writer.writerow({"TaskID": row.get("TaskID"), "URL": url, "Reason": "render_failed"})
                        if args.debug: print(f"[FAIL] render {html_path}")

            browser.close()

    out_csv = Path(args.csv_file).with_name(Path(args.csv_file).stem + "_finalized_offline.csv")
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} and saved PDFs in {outdir}\nHTML cache in {html_cache}")

if __name__ == "__main__":
    main()
