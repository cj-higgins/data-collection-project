#!/usr/bin/env python3
"""build_manifest_dual.py with hard-coded User-Agent

Generates:
  - manifest_ab.csv  : one 10-K per company (>= min-date) for A/B tasks
  - manifest_two.csv : up to two 10-Ks per company (>= min-date) for C (YoY) candidates

Usage:
  python build_manifest_dual.py --input companies_80.csv --tickers company_tickers.json --min-date 2023-10-01

The SEC requires a User-Agent with contact info. This script now defaults to:
  "TrialDataCollection/1.0 (REPLACE_WITH_CONTACT@example.com)"
"""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

DEFAULT_FORMS = ["10-K"]
DEFAULT_UA = "TrialDataCollection/1.0 (REPLACE_WITH_CONTACT@example.com)"

def build_session() -> requests.Session:
    headers = {"User-Agent": DEFAULT_UA, "Accept-Encoding": "gzip, deflate"}
    session = requests.Session()
    session.headers.update(headers)
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def load_ticker_map(tickers_json: Path) -> Dict[str, str]:
    raw = tickers_json.read_text(encoding="utf-8")
    data = json.loads(raw)
    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            rows = data["data"]
            fields = data.get("fields")
            if fields and all(isinstance(r, list) for r in rows):
                idx_ticker = fields.index("ticker") if "ticker" in fields else None
                idx_cik = fields.index("cik_str") if "cik_str" in fields else None
                if idx_ticker is not None and idx_cik is not None:
                    return {r[idx_ticker].upper(): str(r[idx_cik]).zfill(10) for r in rows}
            data = rows
        else:
            data = list(data.values())
    mapping = {}
    for row in data:
        if isinstance(row, dict) and "ticker" in row and "cik_str" in row:
            mapping[row["ticker"].upper()] = str(row["cik_str"]).zfill(10)
    if not mapping:
        raise ValueError("Unable to parse company_tickers.json into a ticker->CIK map")
    return mapping

def fetch_submissions(session: requests.Session, cik: str) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def pick_filings(data: dict, forms: List[str], min_date: str, max_per_company: int) -> List[dict]:
    recent = data.get("filings", {}).get("recent", {})
    forms_list = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])
    rows: List[dict] = []
    for form, fdate, accn, repdate, primary in zip(forms_list, filing_dates, accessions, report_dates, primary_docs):
        if form not in forms:
            continue
        if not isinstance(fdate, str) or fdate < min_date:
            continue
        rows.append({
            "Form": form,
            "FilingDate": fdate,
            "Accession": accn,
            "Period": repdate or (fdate[:4] if isinstance(fdate, str) and len(fdate) >= 4 else ""),
            "PrimaryDocument": primary,
        })
        if len(rows) >= max_per_company:
            break
    return rows

def index_url(cik: str, accession: str) -> str:
    acc_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{acc_nodash}-index.html"

def open_html_url(cik: str, accession: str, primary_doc: str) -> str:
    acc_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{primary_doc}"

def write_manifest(out_path: Path, rows_out: List[dict]) -> None:
    out_df = pd.DataFrame(rows_out, columns=[
        "TaskID","Company","Ticker","Sector","Form","Period","FilingDate","Accession","EdgarIndexURL","OpenAsHTMLURL"
    ])
    out_df.to_csv(out_path, index=False)

def build_dual(input_csv: Path, tickers_json: Path, min_date: str, forms: List[str]) -> None:
    session = build_session()
    ticker_map = load_ticker_map(tickers_json)
    df = pd.read_csv(input_csv)
    required_cols = {"Company","Ticker","Sector"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise SystemExit(f"Missing columns in input CSV: {missing}")

    ab_rows: List[dict] = []
    two_rows: List[dict] = []
    errors: List[str] = []

    for _, r in df.iterrows():
        company = str(r["Company"]).strip()
        ticker = str(r["Ticker"]).strip().upper()
        sector = str(r["Sector"]).strip()

        cik = ticker_map.get(ticker)
        if not cik:
            errors.append(f"No CIK mapping for {ticker}")
            continue

        try:
            data = fetch_submissions(session, cik)
            picks_ab = pick_filings(data, forms=forms, min_date=min_date, max_per_company=1)
            for p in picks_ab:
                ab_rows.append({
                    "TaskID": "",
                    "Company": company, "Ticker": ticker, "Sector": sector,
                    "Form": p["Form"], "Period": p["Period"], "FilingDate": p["FilingDate"],
                    "Accession": p["Accession"],
                    "EdgarIndexURL": index_url(cik, p["Accession"]),
                    "OpenAsHTMLURL": open_html_url(cik, p["Accession"], p["PrimaryDocument"]),
                })
            picks_two = pick_filings(data, forms=forms, min_date=min_date, max_per_company=2)
            for p in picks_two:
                two_rows.append({
                    "TaskID": "",
                    "Company": company, "Ticker": ticker, "Sector": sector,
                    "Form": p["Form"], "Period": p["Period"], "FilingDate": p["FilingDate"],
                    "Accession": p["Accession"],
                    "EdgarIndexURL": index_url(cik, p["Accession"]),
                    "OpenAsHTMLURL": open_html_url(cik, p["Accession"], p["PrimaryDocument"]),
                })

        except requests.HTTPError as e:
            errors.append(f"HTTP error for {ticker} (cik {cik}): {e}")
        except Exception as e:
            errors.append(f"Error for {ticker} (cik {cik}): {e}")
        finally:
            time.sleep(0.2)

    write_manifest(Path("manifest_ab.csv"), ab_rows)
    write_manifest(Path("manifest_two.csv"), two_rows)

    if errors:
        Path("manifest_errors.log").write_text("\n".join(errors), encoding="utf-8")
        logging.info("Completed with %d warnings. See manifest_errors.log", len(errors))
    else:
        logging.info("Completed with no warnings.")

    logging.info("Wrote %d rows to manifest_ab.csv and %d rows to manifest_two.csv", len(ab_rows), len(two_rows))

def parse_args():
    ap = argparse.ArgumentParser(description="Build A/B and TWO manifests from a company list")
    ap.add_argument("--input", type=Path, required=True, help="Input CSV with Company,Ticker,Sector")
    ap.add_argument("--tickers", type=Path, required=True, help="Path to company_tickers.json")
    ap.add_argument("--min-date", type=str, default="2023-10-01", help="Minimum filing date YYYY-MM-DD (default: 2023-10-01)")
    ap.add_argument("--forms", type=str, default="10-K", help="Comma-separated forms (default: 10-K)")
    return ap.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    forms = [s.strip().upper() for s in args.forms.split(",") if s.strip()]
    build_dual(args.input, args.tickers, args.min_date, forms=forms)
