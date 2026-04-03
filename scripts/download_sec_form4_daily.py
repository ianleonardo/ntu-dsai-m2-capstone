#!/usr/bin/env python3
"""
Download SEC Form 4 filings (no 4/A), parse ownership XML, and write monthly TSVs.

Outputs (by month, YYYY-MM):
  - SUBMISSION_YYYY-MM.tsv
  - REPORTINGOWNER_YYYY-MM.tsv
  - NONDERIV_TRANS_YYYY-MM.tsv

Optional `--upload-bigquery` appends TSVs then runs the same post-load dedupe as
`scripts/download_sec_to_bigquery.py` (one row per primary key; prefers max `year`
when present). Set `SEC_SKIP_DEDUPE=1` to skip dedupe.
"""

# NOTE: this file was renamed from download_sec_form4_monthly.py

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv

try:
    from google.cloud import bigquery
except Exception:  # noqa: BLE001
    bigquery = None

try:
    from scripts.download_sec_to_bigquery import SECBigQueryLoader, TABLE_CONFIGS
except ImportError:  # noqa: BLE001
    SECBigQueryLoader = None  # type: ignore[misc, assignment]
    TABLE_CONFIGS = None  # type: ignore[misc, assignment]


DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parent.parent
    / "dataprocessing"
    / "meltano_ingestion"
    / "staging"
    / "sec_form4_2026_monthly"
)

DEFAULT_UA = "NTU-DSAI-Capstone SEC Form4 Downloader (set --user-agent)"
SEC_BASE = "https://www.sec.gov/Archives"

SUBMISSION_COLUMNS = [
    "ACCESSION_NUMBER",
    "CIK",
    "FILING_DATE",
    "PERIOD_OF_REPORT",
    "DOCUMENT_TYPE",
    "ISSUERCIK",
    "ISSUERNAME",
    "ISSUERTRADINGSYMBOL",
    "NOT_SUBJECT_SEC16",
    "SOURCE_FILE",
]

REPORTINGOWNER_COLUMNS = [
    "ACCESSION_NUMBER",
    "RPTOWNERCIK",
    "RPTOWNERNAME",
    "RPTOWNER_RELATIONSHIP",
    "IS_DIRECTOR",
    "IS_OFFICER",
    "IS_TENPERCENTOWNER",
    "IS_OTHER",
    # SEC / Meltano bulk name (matches BigQuery `sec_reportingowner`)
    "RPTOWNER_TITLE",
]

NONDERIV_COLUMNS = [
    "ACCESSION_NUMBER",
    "NONDERIV_TRANS_SK",
    "SECURITY_TITLE",
    "TRANS_DATE",
    "DEEMED_EXECUTION_DATE",
    "TRANS_FORM_TYPE",
    "TRANS_CODE",
    "EQUITY_SWAP_INVOLVED",
    "TRANS_SHARES",
    "TRANS_PRICEPERSHARE",
    "TRANS_ACQUIRED_DISP_CD",
    # SEC / Meltano bulk name (matches `fct_sec_nonderiv_line` source)
    "SHRS_OWND_FOLWNG_TRANS",
    "DIRECT_INDIRECT_OWNERSHIP",
    "NATURE_OF_OWNERSHIP",
]

# Map legacy/local column names from older TSVs to SEC names when uploading to BigQuery.
FORM4_BQ_COLUMN_ALIASES: Dict[str, Dict[str, str]] = {
    "sec_reportingowner": {"OFFICER_TITLE": "RPTOWNER_TITLE"},
    "sec_nonderiv_trans": {"SHARES_OWNED_FOLLOWING_TRANS": "SHRS_OWND_FOLWNG_TRANS"},
}

TABLE_TO_COLUMNS = {
    "sec_submission": SUBMISSION_COLUMNS,
    "sec_reportingowner": REPORTINGOWNER_COLUMNS,
    "sec_nonderiv_trans": NONDERIV_COLUMNS,
}


def quarter_for(d: date) -> str:
    return f"QTR{((d.month - 1) // 3) + 1}"


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def date_iter(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def to_dd_mmm_yyyy(value: str) -> str:
    """Normalize date-ish strings to DD-MMM-YYYY (e.g. 02-JAN-2026)."""
    s = (value or "").strip()
    if not s:
        return ""
    # Already DD-MMM-YYYY
    try:
        dt = datetime.strptime(s, "%d-%b-%Y").date()
        return dt.strftime("%d-%b-%Y").upper()
    except ValueError:
        pass
    # SEC XML commonly uses ISO date
    try:
        dt = datetime.strptime(s, "%Y-%m-%d").date()
        return dt.strftime("%d-%b-%Y").upper()
    except ValueError:
        pass
    # Daily index date
    try:
        dt = datetime.strptime(s, "%Y%m%d").date()
        return dt.strftime("%d-%b-%Y").upper()
    except ValueError:
        pass
    # Keep original when format is unknown
    return s


class RateLimitedSession:
    def __init__(self, user_agent: str, max_rps: float, extra_sleep: float) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov",
            }
        )
        self.min_interval = 1.0 / max(max_rps, 0.1)
        self.extra_sleep = max(extra_sleep, 0.0)
        self._last_request_ts = 0.0

    def get_text(self, url: str, timeout: int = 60, retries: int = 3) -> str:
        last_exc: Optional[Exception] = None
        for i in range(retries):
            now = time.time()
            wait_s = self.min_interval - (now - self._last_request_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            if self.extra_sleep > 0:
                time.sleep(self.extra_sleep)
            try:
                resp = self.session.get(url, timeout=timeout)
                self._last_request_ts = time.time()
                if resp.status_code == 404:
                    raise FileNotFoundError(url)
                resp.raise_for_status()
                return resp.text
            except FileNotFoundError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                backoff = min(2**i, 10)
                time.sleep(backoff)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"GET failed: {url}")

    def get_bytes(self, url: str, timeout: int = 60, retries: int = 3) -> bytes:
        last_exc: Optional[Exception] = None
        for i in range(retries):
            now = time.time()
            wait_s = self.min_interval - (now - self._last_request_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            if self.extra_sleep > 0:
                time.sleep(self.extra_sleep)
            try:
                resp = self.session.get(url, timeout=timeout)
                self._last_request_ts = time.time()
                if resp.status_code == 404:
                    raise FileNotFoundError(url)
                resp.raise_for_status()
                return resp.content
            except FileNotFoundError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                backoff = min(2**i, 10)
                time.sleep(backoff)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"GET failed: {url}")


def safe_text(node: Optional[ET.Element], path: str) -> str:
    if node is None:
        return ""
    child = node.find(path)
    if child is None or child.text is None:
        return ""
    # Keep TSV one-record-per-line: flatten embedded newlines/tabs from SEC free-text fields.
    text = child.text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return " ".join(text.split())


def true01(v: str) -> str:
    t = v.strip().lower()
    if t in ("1", "true", "y", "yes"):
        return "1"
    if t in ("0", "false", "n", "no"):
        return "0"
    return ""


def parse_master_idx(text: str) -> List[Tuple[str, str, str, str, str]]:
    rows: List[Tuple[str, str, str, str, str]] = []
    start = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("CIK|Company Name|Form Type|Date Filed|File Name"):
            start = True
            continue
        if not start:
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        rows.append((parts[0], parts[1], parts[2], parts[3], parts[4]))
    return rows


def fetch_daily_master(session: RateLimitedSession, d: date) -> Optional[str]:
    base = f"{SEC_BASE}/edgar/daily-index/{d.year}/{quarter_for(d)}"
    idx_url = f"{base}/master.{d.strftime('%Y%m%d')}.idx"
    gz_url = f"{idx_url}.gz"
    text: Optional[str] = None

    try:
        text = session.get_text(idx_url, timeout=45)
    except FileNotFoundError:
        text = None
    except Exception:
        text = None

    if text and "CIK|Company Name|Form Type|Date Filed|File Name" in text:
        return text

    try:
        gz_bytes = session.get_bytes(gz_url, timeout=45)
        decompressed = gzip.decompress(gz_bytes)
        text_gz = decompressed.decode("latin-1", errors="replace")
        if "CIK|Company Name|Form Type|Date Filed|File Name" in text_gz:
            return text_gz
    except FileNotFoundError:
        return None
    except Exception:
        return None

    return None


def extract_accession_from_filename(file_name: str) -> str:
    base = file_name.rsplit("/", 1)[-1]
    return base.replace(".txt", "")


def extract_xml_block(filing_txt: str) -> Optional[str]:
    start = filing_txt.find("<XML>")
    end = filing_txt.find("</XML>")
    if start == -1 or end == -1 or end <= start:
        return None
    return filing_txt[start + len("<XML>") : end].strip()


@dataclass
class ParsedFiling:
    submission_row: Dict[str, str]
    reporting_rows: List[Dict[str, str]]
    nonderiv_rows: List[Dict[str, str]]


def parse_ownership_xml(
    xml_text: str, accession: str, cik: str, filing_date: str, source_file: str
) -> ParsedFiling:
    root = ET.fromstring(xml_text)

    issuer = root.find("issuer")
    submission_row = {
        "ACCESSION_NUMBER": accession,
        "CIK": cik,
        "FILING_DATE": to_dd_mmm_yyyy(filing_date),
        "PERIOD_OF_REPORT": to_dd_mmm_yyyy(safe_text(root, "periodOfReport")),
        "DOCUMENT_TYPE": safe_text(root, "documentType"),
        "ISSUERCIK": safe_text(issuer, "issuerCik"),
        "ISSUERNAME": safe_text(issuer, "issuerName"),
        "ISSUERTRADINGSYMBOL": safe_text(issuer, "issuerTradingSymbol"),
        "NOT_SUBJECT_SEC16": true01(safe_text(root, "notSubjectToSection16")),
        "SOURCE_FILE": source_file,
    }

    reporting_rows: List[Dict[str, str]] = []
    for ro in root.findall("reportingOwner"):
        ro_id = ro.find("reportingOwnerId")
        rel = ro.find("reportingOwnerRelationship")
        is_director = true01(safe_text(rel, "isDirector"))
        is_officer = true01(safe_text(rel, "isOfficer"))
        is_ten = true01(safe_text(rel, "isTenPercentOwner"))
        is_other = true01(safe_text(rel, "isOther"))
        relationship_bits = []
        if is_director == "1":
            relationship_bits.append("DIRECTOR")
        if is_officer == "1":
            relationship_bits.append("OFFICER")
        if is_ten == "1":
            relationship_bits.append("TENPERCENTOWNER")
        if is_other == "1":
            relationship_bits.append("OTHER")
        reporting_rows.append(
            {
                "ACCESSION_NUMBER": accession,
                "RPTOWNERCIK": safe_text(ro_id, "rptOwnerCik"),
                "RPTOWNERNAME": safe_text(ro_id, "rptOwnerName"),
                "RPTOWNER_RELATIONSHIP": ",".join(relationship_bits),
                "IS_DIRECTOR": is_director,
                "IS_OFFICER": is_officer,
                "IS_TENPERCENTOWNER": is_ten,
                "IS_OTHER": is_other,
                "RPTOWNER_TITLE": safe_text(rel, "officerTitle"),
            }
        )

    nonderiv_rows: List[Dict[str, str]] = []
    non_table = root.find("nonDerivativeTable")
    if non_table is not None:
        for i, tr in enumerate(non_table.findall("nonDerivativeTransaction"), start=1):
            sec_title = tr.find("securityTitle")
            trans_date = tr.find("transactionDate")
            deemed_date = tr.find("deemedExecutionDate")
            coding = tr.find("transactionCoding")
            amounts = tr.find("transactionAmounts")
            post = tr.find("postTransactionAmounts")
            own = tr.find("ownershipNature")

            nonderiv_rows.append(
                {
                    "ACCESSION_NUMBER": accession,
                    "NONDERIV_TRANS_SK": str(i),
                    "SECURITY_TITLE": safe_text(sec_title, "value"),
                    "TRANS_DATE": to_dd_mmm_yyyy(safe_text(trans_date, "value")),
                    "DEEMED_EXECUTION_DATE": to_dd_mmm_yyyy(safe_text(deemed_date, "value")),
                    "TRANS_FORM_TYPE": safe_text(coding, "transactionFormType"),
                    "TRANS_CODE": safe_text(coding, "transactionCode"),
                    "EQUITY_SWAP_INVOLVED": true01(safe_text(coding, "equitySwapInvolved")),
                    "TRANS_SHARES": safe_text(
                        amounts.find("transactionShares") if amounts is not None else None,
                        "value",
                    ),
                    "TRANS_PRICEPERSHARE": safe_text(
                        amounts.find("transactionPricePerShare") if amounts is not None else None,
                        "value",
                    ),
                    "TRANS_ACQUIRED_DISP_CD": safe_text(
                        amounts.find("transactionAcquiredDisposedCode")
                        if amounts is not None
                        else None,
                        "value",
                    ),
                    "SHRS_OWND_FOLWNG_TRANS": safe_text(
                        post.find("sharesOwnedFollowingTransaction") if post is not None else None,
                        "value",
                    ),
                    "DIRECT_INDIRECT_OWNERSHIP": safe_text(
                        own.find("directOrIndirectOwnership") if own is not None else None,
                        "value",
                    ),
                    "NATURE_OF_OWNERSHIP": safe_text(
                        own.find("natureOfOwnership") if own is not None else None, "value"
                    ),
                }
            )

    return ParsedFiling(
        submission_row=submission_row,
        reporting_rows=reporting_rows,
        nonderiv_rows=nonderiv_rows,
    )


def month_key_from_filing_date(filing_date: str) -> str:
    d = datetime.strptime(filing_date, "%Y-%m-%d").date()
    return d.strftime("%Y-%m")


def ensure_tsv(path: Path, columns: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()


def append_rows(path: Path, columns: List[str], rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    ensure_tsv(path, columns)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t", extrasaction="ignore")
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def month_state_path(output_dir: Path, month_key: str) -> Path:
    return output_dir / f"state_{month_key}.json"


def load_processed(output_dir: Path, month_key: str) -> Set[str]:
    p = month_state_path(output_dir, month_key)
    if not p.exists():
        return set()
    data = json.loads(p.read_text(encoding="utf-8"))
    return set(data.get("processed_accessions", []))


def save_processed(output_dir: Path, month_key: str, processed: Set[str]) -> None:
    p = month_state_path(output_dir, month_key)
    p.write_text(
        json.dumps({"month": month_key, "processed_accessions": sorted(processed)}, indent=2),
        encoding="utf-8",
    )


def read_tsv_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows: List[Dict[str, str]] = []
        for r in reader:
            rows.append(
                {k: (v if v is not None else "") for k, v in r.items() if k is not None}
            )
        return rows


def _canonicalize_row_for_bq(row: Dict[str, str], table_name: str) -> Dict[str, str]:
    """Apply legacy column aliases so Form 4 keys match SEC/Meltano BigQuery names."""
    out = dict(row)
    for old, new in FORM4_BQ_COLUMN_ALIASES.get(table_name, {}).items():
        if old in out and new not in out:
            out[new] = out[old]
    return out


def _flat_schema_field_names(schema: List[bigquery.SchemaField]) -> List[str]:
    names: List[str] = []
    for f in schema:
        if f.field_type == "RECORD":
            continue
        names.append(f.name)
    return names


def _build_tsv_bytes_aligned_to_schema(
    rows: List[Dict[str, str]], field_names: List[str]
) -> bytes:
    """TSV body: header + rows in exact BigQuery column order (name-based mapping)."""

    def _cell(v: str) -> str:
        s = v or ""
        return s.replace("\t", " ").replace("\r", " ").replace("\n", " ")

    out_lines = ["\t".join(field_names)]
    for r in rows:
        out_lines.append("\t".join(_cell(r.get(fn, "") or "") for fn in field_names))
    return ("\n".join(out_lines) + "\n").encode("utf-8")


def _validate_monthly_tsv_accession_alignment(output_dir: Path, month: str) -> None:
    """Check SUBMISSION vs REPORTINGOWNER / NONDERIV TSVs before BigQuery upload.

    Reporting-owner rows are required for every submission (same parse batch).

    Non-derivative lines are optional per filing: some Form 4s have an empty
    ``nonDerivativeTable`` (derivatives-only or edge cases), so submission
    accessions may legitimately be absent from NONDERIV_TRANS — warn only.
    """
    sub_fp = output_dir / f"SUBMISSION_{month}.tsv"
    ro_fp = output_dir / f"REPORTINGOWNER_{month}.tsv"
    nd_fp = output_dir / f"NONDERIV_TRANS_{month}.tsv"

    def _acc_set(fp: Path) -> Set[str]:
        return {
            (r.get("ACCESSION_NUMBER") or "").strip()
            for r in read_tsv_dicts(fp)
            if (r.get("ACCESSION_NUMBER") or "").strip()
        }

    s_acc = _acc_set(sub_fp)
    if not s_acc:
        return
    r_acc = _acc_set(ro_fp)
    n_acc = _acc_set(nd_fp)
    if not r_acc:
        raise RuntimeError(
            f"{month}: SUBMISSION has {len(s_acc)} accessions but REPORTINGOWNER TSV is empty; "
            "re-run download or fix files before BigQuery upload."
        )
    if not n_acc:
        print(
            f"[warn] {month}: SUBMISSION has accessions but NONDERIV_TRANS has no rows; "
            "uploading submission/reporting only."
        )
    missing_ro = s_acc - r_acc
    if missing_ro:
        ex = next(iter(missing_ro))
        raise RuntimeError(
            f"{month}: {len(missing_ro)} SUBMISSION accessions missing from REPORTINGOWNER "
            f"(example: {ex})."
        )
    missing_nd = s_acc - n_acc
    if missing_nd:
        ex = next(iter(missing_nd))
        print(
            f"[warn] {month}: {len(missing_nd)} submission accessions have no non-derivative "
            f"lines (e.g. {ex}); expected for some Form 4 filings."
        )


def append_failure(output_dir: Path, month_key: str, accession: str, file_name: str, reason: str) -> None:
    fp = output_dir / f"failures_{month_key}.tsv"
    cols = ["MONTH", "ACCESSION_NUMBER", "FILE_NAME", "REASON"]
    ensure_tsv(fp, cols)
    append_rows(
        fp,
        cols,
        [
            {
                "MONTH": month_key,
                "ACCESSION_NUMBER": accession,
                "FILE_NAME": file_name,
                "REASON": reason,
            }
        ],
    )


def upload_monthly_tsvs_to_bigquery(
    output_dir: Path,
    start_date: date,
    end_date: date,
    project_id: str,
    dataset: str,
) -> Dict[str, int]:
    if bigquery is None:
        raise RuntimeError(
            "google-cloud-bigquery is not installed in this environment; cannot upload to BigQuery."
        )
    if SECBigQueryLoader is None or TABLE_CONFIGS is None:
        raise RuntimeError(
            "Cannot import SECBigQueryLoader from scripts.download_sec_to_bigquery; "
            "run from the project root with the package installed (e.g. uv run)."
        )
    client = bigquery.Client(project=project_id)
    loaded_counts: Dict[str, int] = {
        "sec_submission": 0,
        "sec_reportingowner": 0,
        "sec_nonderiv_trans": 0,
    }

    months: Set[str] = set()
    for d in date_iter(start_date, end_date):
        months.add(d.strftime("%Y-%m"))

    for month in sorted(months):
        file_map = {
            "sec_submission": output_dir / f"SUBMISSION_{month}.tsv",
            "sec_reportingowner": output_dir / f"REPORTINGOWNER_{month}.tsv",
            "sec_nonderiv_trans": output_dir / f"NONDERIV_TRANS_{month}.tsv",
        }
        # Fail fast if local monthly files are inconsistent (common cause of “submission only” in BQ).
        _validate_monthly_tsv_accession_alignment(output_dir, month)

        for table_name, fp in file_map.items():
            if not fp.exists():
                continue
            raw_rows = read_tsv_dicts(fp)
            row_count = len(raw_rows)
            if row_count == 0:
                continue

            table_id = f"{project_id}.{dataset}.{table_name}"

            # Avoid schema drift on append: use existing table schema when present.
            try:
                existing_table = client.get_table(table_id)
                schema = list(existing_table.schema)
                autodetect = False
            except Exception:  # noqa: BLE001
                schema = [
                    bigquery.SchemaField(col, "STRING", mode="NULLABLE")
                    for col in TABLE_TO_COLUMNS[table_name]
                ]
                autodetect = False

            field_names = _flat_schema_field_names(schema)
            rows_for_load = [_canonicalize_row_for_bq(r, table_name) for r in raw_rows]
            # Only map columns that exist on the destination table; extras (e.g. IS_*) are dropped.
            body = _build_tsv_bytes_aligned_to_schema(rows_for_load, field_names)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter="\t",
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=autodetect,
                schema=schema,
                allow_jagged_rows=True,
                allow_quoted_newlines=True,
            )
            buf = io.BytesIO(body)
            job = client.load_table_from_file(buf, table_id, job_config=job_config)
            job.result()
            loaded_counts[table_name] += row_count

    # Same dedupe as zip / direct SEC load: one row per natural key (TABLE_CONFIGS).
    loader = SECBigQueryLoader(project_id, dataset)
    if not loader.skip_dedupe:
        for cfg in TABLE_CONFIGS.values():
            tid = cfg["table_id"]
            if loaded_counts.get(tid, 0) <= 0:
                continue
            if not loader.dedupe_table(tid, cfg["primary_keys"]):
                raise RuntimeError(f"BigQuery dedupe failed for {project_id}.{dataset}.{tid}")

    return loaded_counts


def run(args: argparse.Namespace) -> int:
    if args.max_requests_per_second > 10:
        print("Error: --max-requests-per-second must be <= 10 per SEC fair access policy.")
        return 2

    user_agent = args.user_agent.strip()
    if not user_agent:
        print("Error: --user-agent must be non-empty.")
        return 2
    if user_agent == DEFAULT_UA:
        print("Warning: Using default User-Agent. Prefer a real contact string.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date > end_date:
        print("Error: --start-date must be <= --end-date")
        return 2

    sess = RateLimitedSession(
        user_agent=user_agent,
        max_rps=args.max_requests_per_second,
        extra_sleep=args.sleep_seconds,
    )

    total_discovered = 0
    total_processed = 0

    for d in date_iter(start_date, end_date):
        idx_text = fetch_daily_master(sess, d)
        if not idx_text:
            # no index published for this date, or source not accessible in this environment
            continue

        rows = parse_master_idx(idx_text)
        wanted_date = d.strftime("%Y%m%d")
        form4_rows = [r for r in rows if r[2].strip() == "4" and r[3].strip() == wanted_date]
        total_discovered += len(form4_rows)
        if not form4_rows:
            continue

        month_key = d.strftime("%Y-%m")
        processed = load_processed(output_dir, month_key) if args.resume else set()

        for cik, _company, _form, filing_date_raw, file_name in form4_rows:
            accession = extract_accession_from_filename(file_name)
            if accession in processed:
                continue
            filing_date = datetime.strptime(filing_date_raw.strip(), "%Y%m%d").date().isoformat()
            if args.dry_run:
                print(f"[dry-run] {filing_date} {accession} {file_name}")
                continue

            filing_url = f"https://www.sec.gov/Archives/{file_name}"
            try:
                filing_txt = sess.get_text(filing_url, timeout=60)
                xml_text = extract_xml_block(filing_txt)
                if not xml_text:
                    raise ValueError("No <XML> block found in filing text")
                parsed = parse_ownership_xml(
                    xml_text=xml_text,
                    accession=accession,
                    cik=cik.strip(),
                    filing_date=filing_date,
                    source_file=file_name,
                )

                append_rows(
                    output_dir / f"SUBMISSION_{month_key}.tsv",
                    SUBMISSION_COLUMNS,
                    [parsed.submission_row],
                )
                append_rows(
                    output_dir / f"REPORTINGOWNER_{month_key}.tsv",
                    REPORTINGOWNER_COLUMNS,
                    parsed.reporting_rows,
                )
                append_rows(
                    output_dir / f"NONDERIV_TRANS_{month_key}.tsv",
                    NONDERIV_COLUMNS,
                    parsed.nonderiv_rows,
                )
                processed.add(accession)
                save_processed(output_dir, month_key, processed)
                total_processed += 1
            except Exception as exc:  # noqa: BLE001
                append_failure(output_dir, month_key, accession, file_name, str(exc))
                print(f"[warn] failed {accession}: {exc}")

    summary = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "form_type": "4",
        "total_discovered": total_discovered,
        "total_processed": total_processed,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if args.upload_bigquery:
        project_id = args.bq_project_id or os.getenv("GOOGLE_PROJECT_ID", "").strip()
        dataset = args.bq_dataset or os.getenv("BIGQUERY_DATASET", "insider_transactions").strip()
        if not project_id:
            print("Error: --upload-bigquery requires --bq-project-id or GOOGLE_PROJECT_ID.")
            return 2
        loaded_counts = upload_monthly_tsvs_to_bigquery(
            output_dir=output_dir,
            start_date=start_date,
            end_date=end_date,
            project_id=project_id,
            dataset=dataset,
        )
        summary["bigquery_upload"] = {
            "project_id": project_id,
            "dataset": dataset,
            "loaded_rows": loaded_counts,
        }

    (output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download SEC Form 4 filings for 2026 and write monthly TSV files."
    )
    p.add_argument("--start-date", default="2026-01-01", help="Start date YYYY-MM-DD.")
    p.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD (default: today).",
    )
    p.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for monthly TSV files.",
    )
    p.add_argument(
        "--user-agent",
        default=DEFAULT_UA,
        help="SEC-compliant user agent string, e.g. 'Your Name your@email.com'.",
    )
    p.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Extra sleep after each request.",
    )
    p.add_argument(
        "--max-requests-per-second",
        type=float,
        default=5.0,
        help="Throttle requests (must be <= 10).",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Resume mode: skip accessions recorded in monthly state files.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Only discover and print filings; do not write TSV rows.",
    )
    p.add_argument(
        "--upload-bigquery",
        action="store_true",
        help="After monthly TSV generation, append rows to BigQuery tables.",
    )
    p.add_argument(
        "--bq-project-id",
        default="",
        help="BigQuery project id (fallback: GOOGLE_PROJECT_ID env).",
    )
    p.add_argument(
        "--bq-dataset",
        default="",
        help="BigQuery dataset id (fallback: BIGQUERY_DATASET env; default insider_transactions).",
    )
    return p


def main() -> int:
    # Load repo root .env if present so BQ flags can be omitted in local runs.
    repo_root = Path(__file__).resolve().parent.parent
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
    parser = build_parser()
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

