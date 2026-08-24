"""
Extracts COSMIC .tsv.gz files and prints the header + first data row
so we can confirm real column names before writing any DB/parsing code.

Run from: Deployed_Server/backend/data
"""
import gzip
import shutil
from pathlib import Path

COSMIC_DIR = Path("raw/COSMIC")

FILES = [
    "Cosmic_CancerGeneCensus_v104_GRCh38.tsv.gz",
    "Cosmic_MutantCensus_v104_GRCh38.tsv.gz",
]

for fname in FILES:
    gz_path = COSMIC_DIR / fname
    if not gz_path.exists():
        print(f"[SKIP] Not found: {gz_path}")
        continue

    out_path = gz_path.with_suffix("")  # strips ".gz" -> ends in .tsv
    print(f"\nExtracting {fname} -> {out_path.name} ...")

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f_in, \
         open(out_path, "w", encoding="utf-8") as f_out:
        shutil.copyfileobj(f_in, f_out)

    print(f"  Done. Size on disk: {out_path.stat().st_size / (1024*1024):.1f} MB")

    # Preview header + first data row
    with open(out_path, "r", encoding="utf-8") as f:
        header = f.readline().strip()
        first_row = f.readline().strip()

    print(f"  Columns ({len(header.split(chr(9)))} total):")
    print(f"    {header}")
    print(f"  Example row:")
    print(f"    {first_row}")

print("\nDone. Full .tsv files are now sitting next to the .tsv.gz originals in raw/COSMIC/")
