#!/usr/bin/env python3
"""assemble_master.py (defaults wired for your workflow)

Reads:
  - manifest_ab.csv   (one 10-K per company, >= 2023-10-01)
  - manifest_two.csv  (up to two 10-Ks per company, >= 2023-10-01)

Writes:
  - tasks_master.csv  (120 rows by default: A=40, B=40, C(YoY)=10, C(Peer)=10)

Category labels:
  A, B, C (YoY), C (Peer)     # note: C subtype is inside Category cell

Columns (in this exact order):
  TaskID,Status,TaskerName,Category,Prompt,Answer,SupportingFacts,QA_Reviewer,QA_Approval,QA_Final_Approval,Notes,
  Company_1,Ticker_1,Sector_1,Form_1,Period_1,FilingDate_1,
  Company_2,Ticker_2,Sector_2,Form_2,Period_2,FilingDate_2,
  Accession_1,EdgarIndexURL_1,OpenAsHTMLURL_1,PDF_filename_1,PDF_checksum_1,PDF_link_1,
  Accession_2,EdgarIndexURL_2,OpenAsHTMLURL_2,PDF_filename_2,PDF_checksum_2,PDF_link_2

Usage (defaults, no args needed):
  python assemble_master.py

Optional overrides:
  python assemble_master.py --ab manifest_ab.csv --two manifest_two.csv --out tasks_master.csv           --a-count 40 --b-count 40 --c-yoy-count 10 --c-peer-count 10

Pairing rules:
  - YoY: pick companies in manifest_two with two DISTINCT years; choose the two most recent years.
  - Peer: form pairs within sector from manifest_ab, prefer same filing year; avoid companies used for YoY.
"""

import argparse
import sys
import pandas as pd

COLUMNS = [
    "TaskID","Status","TaskerName","Category","Prompt","Answer","SupportingFacts",
    "QA_Reviewer","QA_Approval","QA_Final_Approval","Notes",
    "Company_1","Ticker_1","Sector_1","Form_1","Period_1","FilingDate_1",
    "Company_2","Ticker_2","Sector_2","Form_2","Period_2","FilingDate_2",
    "Accession_1","EdgarIndexURL_1","OpenAsHTMLURL_1","PDF_filename_1","PDF_checksum_1","PDF_link_1",
    "Accession_2","EdgarIndexURL_2","OpenAsHTMLURL_2","PDF_filename_2","PDF_checksum_2","PDF_link_2",
]

def _year(date_str: str) -> str:
    return date_str[:4] if isinstance(date_str, str) and len(date_str) >= 4 else ""

def _require_cols(df: pd.DataFrame, needed):
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Input is missing columns: {missing}")

def make_row_single(task_id: str, cat: str, r: pd.Series) -> dict:
    return {
        "TaskID": task_id, "Status": "", "TaskerName": "", "Category": cat,
        "Prompt": "", "Answer": "", "SupportingFacts": "",
        "QA_Reviewer": "", "QA_Approval": "", "QA_Final_Approval": "", "Notes": "",
        "Company_1": r["Company"], "Ticker_1": r["Ticker"], "Sector_1": r["Sector"],
        "Form_1": r["Form"], "Period_1": r.get("Period", ""), "FilingDate_1": r["FilingDate"],
        "Company_2": "", "Ticker_2": "", "Sector_2": "",
        "Form_2": "", "Period_2": "", "FilingDate_2": "",
        "Accession_1": r["Accession"], "EdgarIndexURL_1": r["EdgarIndexURL"], "OpenAsHTMLURL_1": r["OpenAsHTMLURL"],
        "PDF_filename_1": "", "PDF_checksum_1": "", "PDF_link_1": "",
        "Accession_2": "", "EdgarIndexURL_2": "", "OpenAsHTMLURL_2": "",
        "PDF_filename_2": "", "PDF_checksum_2": "", "PDF_link_2": "",
    }

def make_row_pair(task_id: str, cat_label: str, r1: pd.Series, r2: pd.Series) -> dict:
    return {
        "TaskID": task_id, "Status": "", "TaskerName": "", "Category": cat_label,
        "Prompt": "", "Answer": "", "SupportingFacts": "",
        "QA_Reviewer": "", "QA_Approval": "", "QA_Final_Approval": "", "Notes": "",
        "Company_1": r1["Company"], "Ticker_1": r1["Ticker"], "Sector_1": r1["Sector"],
        "Form_1": r1["Form"], "Period_1": r1.get("Period", ""), "FilingDate_1": r1["FilingDate"],
        "Company_2": r2["Company"], "Ticker_2": r2["Ticker"], "Sector_2": r2["Sector"],
        "Form_2": r2["Form"], "Period_2": r2.get("Period", ""), "FilingDate_2": r2["FilingDate"],
        "Accession_1": r1["Accession"], "EdgarIndexURL_1": r1["EdgarIndexURL"], "OpenAsHTMLURL_1": r1["OpenAsHTMLURL"],
        "PDF_filename_1": "", "PDF_checksum_1": "", "PDF_link_1": "",
        "Accession_2": r2["Accession"], "EdgarIndexURL_2": r2["EdgarIndexURL"], "OpenAsHTMLURL_2": r2["OpenAsHTMLURL"],
        "PDF_filename_2": "", "PDF_checksum_2": "", "PDF_link_2": "",
    }

def pick_yoy(two_df: pd.DataFrame, k: int) -> list[dict]:
    out = []
    df = two_df.copy()
    df["Year"] = df["FilingDate"].astype(str).str[:4]
    # require at least two distinct years per company
    for (company, ticker, sector), grp in df.groupby(["Company","Ticker","Sector"]):
        years = grp["Year"].dropna().unique().tolist()
        if len(years) < 2:
            continue
        grp = grp.sort_values("FilingDate", ascending=False)
        r1 = grp.iloc[0]
        r2_candidates = grp[grp["Year"] != r1["Year"]]
        if r2_candidates.empty:
            continue
        r2 = r2_candidates.iloc[0]
        out.append((sector, company, r1, r2))
    # deterministic selection
    out.sort(key=lambda x: (x[0], x[1]))
    rows = []
    for i, (_, _, r1, r2) in enumerate(out[:k], start=1):
        rows.append(make_row_pair(f"C_YOY_{i:02d}", "C (YoY)", r1, r2))
    return rows

def pick_peers(ab_df: pd.DataFrame, k: int, used_companies: set[tuple]) -> list[dict]:
    # Prefer pairs within the same sector and same year; avoid companies used in YoY
    out = []
    df = ab_df.copy()
    df["Year"] = df["FilingDate"].astype(str).str[:4]
    df = df[~df.apply(lambda r: (r["Company"], r["Ticker"]) in used_companies, axis=1)]
    # Stage 1: same sector + same year
    for sector, g in df.groupby("Sector"):
        for year, gy in g.groupby("Year"):
            rows = list(gy.sort_values("Company").to_dict("records"))
            for i in range(0, len(rows)-1, 2):
                r1 = pd.Series(rows[i]); r2 = pd.Series(rows[i+1])
                out.append(make_row_pair("PENDING", "C (Peer)", r1, r2))
                if len(out) >= k:
                    for j, row in enumerate(out, start=1):
                        row["TaskID"] = f"C_PEER_{j:02d}"
                    return out
    # Stage 2: same sector, any year
    if len(out) < k:
        for sector, g in df.groupby("Sector"):
            rows = list(g.sort_values(["Year","Company"]).to_dict("records"))
            for i in range(0, len(rows)-1, 2):
                r1 = pd.Series(rows[i]); r2 = pd.Series(rows[i+1])
                out.append(make_row_pair("PENDING", "C (Peer)", r1, r2))
                if len(out) >= k:
                    for j, row in enumerate(out, start=1):
                        row["TaskID"] = f"C_PEER_{j:02d}"
                    return out
    for j, row in enumerate(out, start=1):
        row["TaskID"] = f"C_PEER_{j:02d}"
    return out[:k]

def assemble(
    ab_path: str = "manifest_ab.csv",
    two_path: str = "manifest_two.csv",
    out_path: str = "tasks_master.csv",
    a_count: int = 40,
    b_count: int = 40,
    c_yoy_count: int = 10,
    c_peer_count: int = 10,
) -> None:
    ab = pd.read_csv(ab_path)
    two = pd.read_csv(two_path)
    _require_cols(ab, ["Company","Ticker","Sector","Form","FilingDate","Accession","EdgarIndexURL","OpenAsHTMLURL"])
    _require_cols(two, ["Company","Ticker","Sector","Form","FilingDate","Accession","EdgarIndexURL","OpenAsHTMLURL"])

    # Deterministic sort for A/B selection
    ab_sorted = ab.sort_values(["Sector","Company","FilingDate"], ascending=[True, True, False]).reset_index(drop=True)

    # Build A and B
    a_rows = [make_row_single(f"A_{i+1:02d}", "A", ab_sorted.iloc[i])
              for i in range(min(a_count, len(ab_sorted)))]
    b_start = min(a_count, len(ab_sorted))
    b_end = min(b_start + b_count, len(ab_sorted))
    b_rows = [make_row_single(f"B_{i+1:02d}", "B", ab_sorted.iloc[b_start + i])
              for i in range(max(0, b_end - b_start))]

    # C (YoY)
    yoy_rows = pick_yoy(two, c_yoy_count)
    used_companies = {(r["Company_1"], r["Ticker_1"]) for r in yoy_rows}

    # C (Peer)
    peer_rows = pick_peers(ab_sorted, c_peer_count, used_companies)

    all_rows = a_rows + b_rows + yoy_rows + peer_rows
    master = pd.DataFrame(all_rows, columns=COLUMNS)
    master.to_csv(out_path, index=False)
    print(f"Wrote {len(master)} rows to {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Assemble the 120-row master tasks CSV from manifest files.")
    ap.add_argument("--ab", default="manifest_ab.csv")
    ap.add_argument("--two", default="manifest_two.csv")
    ap.add_argument("--out", default="tasks_master.csv")
    ap.add_argument("--a-count", type=int, default=40)
    ap.add_argument("--b-count", type=int, default=40)
    ap.add_argument("--c-yoy-count", type=int, default=10)
    ap.add_argument("--c-peer-count", type=int, default=10)
    args = ap.parse_args()
    assemble(args.ab, args.two, args.out, args.a_count, args.b_count, args.c_yoy_count, args.c_peer_count)

if __name__ == "__main__":
    main()
