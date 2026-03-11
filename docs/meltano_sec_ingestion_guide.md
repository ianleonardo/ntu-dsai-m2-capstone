# Ingesting SEC Insider Transactions (2024) with Meltano

Based on the SEC's insider transaction data structure (ZIP files containing TSV records), standard REST API Meltano taps (like `tap-rest-api`) are not well-suited out-of-the-box because they expect JSON responses, not ZIP archives.

To properly ingest this data via Meltano for the year 2024, the recommended approach is a two-step process:
1. **Extraction/Unzipping**: A pre-extraction script (or Meltano custom utility) to download and unzip the 2024 datasets.
2. **Ingestion**: Using `tap-csv` to read the extracted flat files and pipe them to your target of choice (e.g., `target-postgres` or `target-jsonl`).

Here is the step-by-step guide to configure this in your Meltano project.

## Step 1: Install `tap-csv` and your preferred Target

In your Meltano project directory (`meltano-ingestion`), add the CSV extractor and a target (we'll use `target-jsonl` for local testing as an example):

```bash
cd meltano-ingestion
meltano add tap-csv
meltano add target-jsonl
```

## Step 2: Create a Download & Extraction Script (Meltano Utility)

## Step 3: Configure `tap-csv` in `meltano.yml`

Now, configure `tap-csv` to point at the directory where the unzipped TSV files reside. 

Open `meltano.yml` and add the `tap-csv` configuration under `plugins.extractors`:

```yaml
plugins:
  extractors:
  - name: tap-csv
    variant: meltanolabs
    pip_url: git+https://github.com/MeltanoLabs/tap-csv.git
    config:
      files:
        - entity: sec_submissions
          path: ../data/sec/2025q1/SUBMISSIONS.tsv
          keys: [ACCESSION_NUMBER] # Example primary key
          delimiter: "\t"
        # Add other TSV files (e.g., TRANSACTIONS.tsv, SIGNATURES.tsv) and quarters as needed
```
*(Note: You will need to list the specific TSV files you want to ingest under the `files` array).*

## Step 4: Run the Ingestion Pipeline

To execute this end-to-end, you first run the download script, then run the Meltano pipeline:

```bash
# 1. Download and extract the 2025 SEC data
python scripts/download_sec.py

# 2. Run the Meltano ELT pipeline
meltano run tap-csv target-jsonl
```

### Alternative: Building a Custom Singer Tap
If you prefer a fully integrated Meltano solution without external scripts, you can build a custom Singer tap using the Meltano Singer SDK (`pip install singer-sdk`). You would implement the download and unzip logic directly inside the tap's `get_records()` method, yielding the parsed TSV rows directly to Meltano. However, the above script + `tap-csv` approach is typically much faster to set up.
