# SEC Filing Data Collection Pipeline

This repository provides a full pipeline for transforming recent SEC filings into a worker-ready task sheet with local PDF copies and shareable Drive links.

## Overview

The flow is:

1. **Build Manifests** (`build_manifest_dual.py`):  
   Pulls recent SEC filings for a set of companies. Produces `manifest_ab.csv` and `manifest_two.csv`.

2. **Assemble Master Sheet** (`assemble_master.py`):  
   Merges manifests into `tasks_master.csv` with 120 tasks (40 A, 40 B, 10 C-YoY, 10 C-Peer).  
   Fields include company info, filing metadata, two doc slots, and placeholders for PDF filenames, checksums, and links.

3. **Finalize PDFs Offline** (`finalize_pdfs_offline.py`):  
   Renders the HTML filings to local PDFs, fills `PDF_filename_*` and `PDF_checksum_*` in a new CSV (`*_finalized_offline.csv`).

4. **Google Drive Apps Script** (`PDF_links.txt`):  
   Upload PDFs (with unchanged filenames) into a Drive folder.  
   The Apps Script matches filenames, sets sharing to “Anyone with link — Viewer,” and fills `PDF_link_1/2` in the Sheet.

```
companies_80.csv
company_tickers.json
      │
      ▼
 build_manifest_dual.py
   ├── manifest_ab.csv
   └── manifest_two.csv
            │
            ▼
      assemble_master.py
         └── tasks_master.csv
                    │
                    ▼
   finalize_pdfs_offline.py
     ├── html_cache/*.html
     ├── SEC PDFs/*.pdf
     └── tasks_master_finalized_offline.csv
                    │
        (upload PDFs to Drive; keep filenames)
                    │
                    ▼
   Google Sheet + Apps Script
     └── Fills PDF_link_1 / PDF_link_2
```

---

## Usage

### 1. Build Manifests

```bash
python build_manifest_dual.py \
  --input companies_80.csv \
  --tickers company_tickers.json \
  --min-date 2023-10-01 \
  --forms 10-K
```

Outputs: `manifest_ab.csv`, `manifest_two.csv`.

### 2. Assemble Master Sheet

```bash
python assemble_master.py \
  --ab manifest_ab.csv \
  --two manifest_two.csv \
  --out tasks_master.csv \
  --a-count 40 --b-count 40 --c-yoy-count 10 --c-peer-count 10
```

Outputs: `tasks_master.csv`.

### 3. Finalize PDFs Offline

```bash
python finalize_pdfs_offline.py tasks_master.csv \
  --outdir "C:/Users/you/OneDrive/Documents/SEC PDFs" \
  --ua "TrialDataCollection/1.0 (your_email@example.com)" \
  --only-missing --overwrite --debug
```

Outputs: PDFs in `SEC PDFs/` and `tasks_master_finalized_offline.csv`.

> **Note**: Replace `your_email@example.com` with your real contact email.  
> The SEC requires a valid User-Agent with contact information when programmatically accessing filings.

### 4. Upload PDFs to Drive and Fill Links

- Upload the PDFs into your target Drive folder **without renaming them**.
- Paste the Apps Script from `PDF_links.txt` into the Google Sheet.
- Run from the **PDF Links** menu:
  - “Set Sharing on Folder: Anyone/Viewer”
  - “Fill Links (only blanks)”

---

## Requirements

- **Python 3.10+**
- Libraries: `pandas`, `requests`, `playwright`
- Initialize Playwright:  
  ```bash
  python -m playwright install chromium
  ```
- Valid SEC **User-Agent** string with your contact email.
- Google Sheet with your final CSV, plus the Apps Script (requires enabling Advanced Drive Service).

---

## Notes

- **Filename identity is critical.** Filenames must match exactly between rendered PDFs and Drive for links to populate.
- **Drive folder ID must be real (not a shortcut)**; enable Advanced Drive service if using shortcuts.
- **Throttling.** Use a valid UA string to avoid SEC blocking. Scripts include retry logic.
- **Debugging.** The offline renderer logs failed rows to `failed_rows.csv`.
