"""
Populates cosmic_genes and cosmic_mutation_counts in tcna_db.

Run from: Deployed_Server/backend/data
Requires: db_conn.py already configured (same one used by populate_db.py)
"""
import sys
import os
import pandas as pd
import pymysql

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))
from db_conn import get_connection  # reuses your existing DB credentials

COSMIC_DIR = "raw/COSMIC"
CENSUS_FILE = os.path.join(COSMIC_DIR, "Cosmic_CancerGeneCensus_v104_GRCh38.tsv")
MUTANT_FILE = os.path.join(COSMIC_DIR, "Cosmic_MutantCensus_v104_GRCh38.tsv")

# Demo gene shortlist -- keeps the huge Mutant Census file manageable.
# Extend this list any time; the script re-scans the whole file per run.
DEMO_GENES = ["TP53", "KRAS", "BRCA1", "BRCA2", "EGFR", "PIK3CA", "PTEN", "APC"]

# Which MUTATION_SOMATIC_STATUS values count as genuinely confirmed somatic.
# (Excludes "Variant of unknown origin" and anything germline-related.)
CONFIRMED_STATUSES = {
    "Confirmed somatic variant",
    "Reported in another cancer sample as somatic",
    "Previously observed as somatic in another sample",
}


def load_gene_census(conn):
    print("Loading Cancer Gene Census reference table...")
    df = pd.read_csv(CENSUS_FILE, sep="\t", dtype=str)
    df = df[df["GENE_SYMBOL"].isin(DEMO_GENES)]

    rows = [
        (
            r["GENE_SYMBOL"],
            r.get("NAME"),
            int(r["TIER"]) if pd.notna(r.get("TIER")) else None,
            r.get("ROLE_IN_CANCER"),
            r.get("MUTATION_TYPES"),
            r.get("TUMOUR_TYPES_SOMATIC"),
        )
        for _, r in df.iterrows()
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO cosmic_genes
                (gene_symbol, gene_name, tier, role_in_cancer, mutation_types, tumour_types_somatic)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                gene_name=VALUES(gene_name),
                tier=VALUES(tier),
                role_in_cancer=VALUES(role_in_cancer),
                mutation_types=VALUES(mutation_types),
                tumour_types_somatic=VALUES(tumour_types_somatic)
            """,
            rows,
        )
    conn.commit()
    print(f"  Inserted/updated {len(rows)} genes.")


def build_mutation_counts(conn):
    print(f"Scanning {MUTANT_FILE} in chunks (this file is large, please be patient)...")

    usecols = ["GENE_SYMBOL", "COSMIC_SAMPLE_ID", "SAMPLE_NAME", "MUTATION_SOMATIC_STATUS", "GENOMIC_MUTATION_ID"]
    chunksize = 200_000
    accumulated = []

    reader = pd.read_csv(MUTANT_FILE, sep="\t", usecols=usecols, dtype=str, chunksize=chunksize)
    total_rows_scanned = 0
    for i, chunk in enumerate(reader):
        total_rows_scanned += len(chunk)
        chunk = chunk[chunk["GENE_SYMBOL"].isin(DEMO_GENES)]
        chunk = chunk[chunk["MUTATION_SOMATIC_STATUS"].isin(CONFIRMED_STATUSES)]
        if not chunk.empty:
            accumulated.append(chunk)
        if (i + 1) % 5 == 0:
            print(f"  Scanned {total_rows_scanned:,} rows so far...")

    print(f"Finished scanning {total_rows_scanned:,} total rows.")

    if not accumulated:
        print("  WARNING: no matching rows found for the demo gene list. Nothing to insert.")
        return

    full = pd.concat(accumulated, ignore_index=True)
    print(f"  {len(full):,} confirmed-somatic mutation rows matched demo genes.")

    # A single true mutation can appear as multiple rows if reported by
    # different studies/publications. Drop duplicates on the actual mutation
    # ID within each (gene, sample) before counting, so we count DISTINCT
    # mutations rather than raw citation rows.
    before = len(full)
    full = full.drop_duplicates(
        subset=["GENE_SYMBOL", "COSMIC_SAMPLE_ID", "GENOMIC_MUTATION_ID"]
    )
    print(f"  Deduplicated {before:,} -> {len(full):,} rows (removed re-reported duplicates).")

    counts = (
        full.groupby(["GENE_SYMBOL", "COSMIC_SAMPLE_ID", "SAMPLE_NAME"])
        .size()
        .reset_index(name="mutation_count")
    )
    print(f"  Aggregated into {len(counts):,} (gene, sample) rows.")

    rows = list(
        counts[["GENE_SYMBOL", "COSMIC_SAMPLE_ID", "SAMPLE_NAME", "mutation_count"]].itertuples(
            index=False, name=None
        )
    )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO cosmic_mutation_counts
                (gene_symbol, cosmic_sample_id, sample_name, mutation_count)
            VALUES (%s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    print(f"  Inserted {len(rows):,} mutation count records.")


if __name__ == "__main__":
    conn = get_connection()
    try:
        load_gene_census(conn)
        build_mutation_counts(conn)
    finally:
        conn.close()
    print("\nDone. cosmic_genes and cosmic_mutation_counts are now populated.")