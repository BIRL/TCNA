import os
import pandas as pd
import numpy as np
import pymysql
import tempfile
from typing import Dict, List, Set, Tuple
from tqdm import tqdm
import sys
# Add the scripts folder to Python path
scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts'))
sys.path.append(scripts_dir)

from DEPTH2 import depth2_calculation
from DEPTH_ITH import depth_calculation
from db_conn import get_connection

# === CONFIGURATION ===
# Path to data directory (relative to script location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data", "raw"))
BATCH_SIZE = 1000000


def normalize_sample_type(sample_type: str) -> str:
    """
    Normalize sample_type from GDC format to simple 'tumor' or 'normal'.
    
    GDC uses various formats like:
    - "primary tumor" -> "tumor"
    - "solid tissue normal" -> "normal"
    
    Args:
        sample_type: Sample type string from GDC
        
    Returns:
        Normalized sample type: "tumor" or "normal"
    """
    if not sample_type:
        return "tumor"  # Default to tumor if empty
    
    sample_type_lower = str(sample_type).strip().lower()
    
    # Check for normal types
    normal_keywords = ['normal', 'control', 'benign']
    if any(keyword in sample_type_lower for keyword in normal_keywords):
        return "normal"
    
    # Everything else is considered tumor
    return "tumor"


def get_cancer_sites_from_directory(data_dir: str) -> List[str]:
    """
    Automatically detect all cancer sites from the data directory.
    Returns: List of cancer site names (directory names)
    """
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    cancer_sites = []
    for item in os.listdir(data_dir):
        item_path = os.path.join(data_dir, item)
        if os.path.isdir(item_path):
            # Check if directory contains CSV files (has at least one expression file)
            has_csv = any(
                os.path.exists(os.path.join(item_path, f"{prefix}_{norm}.csv"))
                for prefix in ["tumor", "normal"]
                for norm in ["tpm", "fpkm", "fpkm_uq"]
            )
            if has_csv:
                cancer_sites.append(item)
    
    return sorted(cancer_sites)


def populate_genes_table(conn, data_dir: str, cancer_names: List[str]) -> Dict[str, int]:
    """
    Extract unique genes/annotations from CSV files and populate genes table.
    Returns: Dictionary mapping ensembl_id -> gene_id
    """
    print("\nPopulating genes table...")
    cur = conn.cursor()
    
    # Collect all unique genes from all CSV files
    all_genes = set()
    
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if not os.path.isdir(folder_path):
            continue
            
        # Check any CSV file to get genes (they all have same genes)
        tpm_path = os.path.join(folder_path, "tumor_tpm.csv")
        if os.path.exists(tpm_path):
            df = pd.read_csv(tpm_path, usecols=['gene_id', 'gene_name'], dtype=str)
            genes_df = df[['gene_id', 'gene_name']].drop_duplicates()
            all_genes.update([tuple(row) for row in genes_df.values])
    
    print(f"Found {len(all_genes):,} unique genes")
    
    # Insert genes into database
    insert_query = """
        INSERT INTO genes (ensembl_id, gene_symbol)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE gene_symbol = VALUES(gene_symbol)
    """
    
    records = [(ensembl_id, gene_symbol) for ensembl_id, gene_symbol in all_genes]
    cur.executemany(insert_query, records)
    conn.commit()
    
    # Fetch gene map
    cur.execute("SELECT id, ensembl_id FROM genes")
    gene_map = {row['ensembl_id']: row['id'] for row in cur.fetchall()}
    
    print(f"Populated {len(gene_map):,} genes")
    return gene_map


def populate_sites_table(conn, data_dir: str, cancer_names: List[str]) -> Dict[str, int]:
    """
    Extract site names from directory structure and populate sites table.
    Includes predefined list of cancer sites.
    Returns: Dictionary mapping site_name -> site_id
    """
    print("\nPopulating sites table...")
    cur = conn.cursor()
    
    # Predefined list of cancer sites (must match preprocess.py exactly)
    predefined_sites = [
        "Adrenal Gland", "Bladder", "Bone Marrow and Blood", "Brain", "Breast", 
        "Cervix", "Colorectal", "Esophagus", "Eye", "Head and Neck", "Kidney", 
        "Liver", "Lung", "Lymph Nodes", "Ovary", "Pancreas", "Pleura", 
        "Prostate", "Rectum", "Skin", "Soft Tissue", "Stomach", "Testis", 
        "Thymus", "Thyroid", "Uterus"
    ]
    
    # Get all site names from directories
    site_names = set(predefined_sites)
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if os.path.isdir(folder_path):
            site_names.add(cancer_name)
    
    print(f"Found {len(site_names):,} sites (including {len(predefined_sites)} predefined)")
    
    # Check which sites already exist in the database
    cur.execute("SELECT name FROM sites")
    existing_sites = {row['name'] for row in cur.fetchall()}
    
    # Only insert sites that don't already exist
    new_sites = site_names - existing_sites
    
    if new_sites:
        print(f"Inserting {len(new_sites):,} new sites...")
        insert_query = """
            INSERT INTO sites (name)
            VALUES (%s)
        """
        cur.executemany(insert_query, [(name,) for name in new_sites])
        conn.commit()
        print(f"  Inserted: {', '.join(sorted(new_sites))}")
    else:
        print("All sites already exist in database. No new sites to insert.")
    
    # Fetch site map
    cur.execute("SELECT id, name FROM sites")
    site_map = {row['name']: row['id'] for row in cur.fetchall()}
    
    print(f"Total sites in database: {len(site_map):,}")
    return site_map


def populate_cancer_types_table(conn, data_dir: str, cancer_names: List[str], 
                                 site_map: Dict[str, int]) -> Dict[str, int]:
    """
    Populate cancer_types table FIRST using sample_sheet.csv from each site folder.
    
    This function must run before populate_samples_table() because samples need cancer_type_id.
    Reads sample_sheet.csv from each site folder and extracts unique TCGA codes.
    Maps TCGA codes to sites based on which folder they appear in.
    
    Returns: Dictionary mapping tcga_code -> cancer_type_id
    """
    print("\n" + "=" * 60)
    print("STEP 3: Populating cancer_types table (using sample_sheet.csv)")
    print("=" * 60)
    cur = conn.cursor()
    
    # Extract TCGA codes from sample sheets and map them to sites
    # Format: {tcga_code: site_id}
    tcga_to_site = {}
    
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if not os.path.isdir(folder_path):
            continue
        
        # Get site_id for this cancer name
        site_id = site_map.get(cancer_name)
        if not site_id:
            print(f"  WARNING: Site ID not found for {cancer_name}, skipping")
            continue
        
        # Read sample_sheet.csv from this site folder
        sample_sheet_path = os.path.join(folder_path, "sample_sheet.csv")
        if not os.path.exists(sample_sheet_path):
            print(f"  WARNING: sample_sheet.csv not found for {cancer_name}, skipping")
            print(f"    Expected path: {sample_sheet_path}")
            continue
        
        try:
            print(f"\n  Processing {cancer_name}...")
            sample_df = pd.read_csv(sample_sheet_path)
            print(f"    Loaded sample_sheet.csv: {len(sample_df)} rows")
            
            # Extract unique TCGA codes from sample sheet
            if 'tcga_code' in sample_df.columns:
                unique_tcga_codes = sample_df['tcga_code'].dropna().unique()
                for tcga_code in unique_tcga_codes:
                    if pd.notna(tcga_code) and str(tcga_code).strip():
                        tcga_code = str(tcga_code).strip()
                        # Map TCGA code to this site (if multiple sites have same TCGA code, last one wins)
                        tcga_to_site[tcga_code] = site_id
                print(f"    Found {len(unique_tcga_codes)} unique TCGA codes: {', '.join(sorted(unique_tcga_codes)[:5])}{'...' if len(unique_tcga_codes) > 5 else ''}")
            else:
                print(f"    ERROR: 'tcga_code' column not found in sample_sheet.csv")
                print(f"    Available columns: {', '.join(sample_df.columns)}")
        except Exception as e:
            print(f"    ERROR reading sample_sheet.csv: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\nFound {len(tcga_to_site):,} unique TCGA codes mapped to sites")
    if len(tcga_to_site) > 0:
        print(f"  Example TCGA codes: {', '.join(sorted(list(tcga_to_site.keys()))[:10])}")
    
    # Insert cancer types with site_id mapping
    insert_query = """
        INSERT INTO cancer_types (tcga_code, site_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE site_id = VALUES(site_id)
    """
    
    records = [(tcga_code, site_id) for tcga_code, site_id in tcga_to_site.items()]
    
    if records:
        cur.executemany(insert_query, records)
        conn.commit()
        print(f"  Inserted/updated {len(records):,} cancer types")
    else:
        print("  WARNING: No TCGA codes found to insert!")
    
    # Fetch cancer_type map
    cur.execute("SELECT id, tcga_code FROM cancer_types")
    cancer_type_map = {row['tcga_code']: row['id'] for row in cur.fetchall()}
    
    print(f"Populated {len(cancer_type_map):,} cancer types in map")
    if len(cancer_type_map) > 0:
        print(f"  Example TCGA codes in map: {', '.join(sorted(list(cancer_type_map.keys()))[:10])}")
    
    return cancer_type_map


def populate_samples_table(conn, data_dir: str, cancer_names: List[str],
                           site_map: Dict[str, int], 
                           cancer_type_map: Dict[str, int]) -> Dict[str, int]:
    """
    Extract sample barcodes from sample_sheet.csv and populate samples table.
    Uses sample sheet to map TCGA codes to cancer_type_id.
    Returns: Dictionary mapping sample_barcode -> sample_id
    """
    print("\nPopulating samples table...")
    cur = conn.cursor()
    
    all_samples = []
    samples_with_null_cancer_type = []
    tcga_codes_found = set()
    
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if not os.path.isdir(folder_path):
            continue
        
        # Read sample_sheet.csv
        sample_sheet_path = os.path.join(folder_path, "sample_sheet.csv")
        if not os.path.exists(sample_sheet_path):
            print(f"  WARNING: sample_sheet.csv not found for {cancer_name}, skipping")
            continue
        
        try:
            sample_df = pd.read_csv(sample_sheet_path)
            
            # Check required columns
            required_columns = ['sample_barcode', 'sample_type']
            if not all(col in sample_df.columns for col in required_columns):
                print(f"  WARNING: Missing required columns in sample_sheet.csv for {cancer_name}")
                continue
            
            # Process each sample from sample sheet
            for _, row in sample_df.iterrows():
                sample_barcode = str(row['sample_barcode']).strip()
                sample_type_raw = str(row['sample_type']).strip()
                
                # Normalize sample_type to "tumor" or "normal"
                sample_type = normalize_sample_type(sample_type_raw)
                
                # Get TCGA code from sample sheet
                tcga_code = None
                if 'tcga_code' in sample_df.columns:
                    tcga_code_val = row.get('tcga_code')
                    if pd.notna(tcga_code_val):
                        tcga_code = str(tcga_code_val).strip()
                
                # If no tcga_code column or value, try to extract from sample_barcode
                if not tcga_code:
                    parts = sample_barcode.split('-')
                    if len(parts) >= 2 and parts[0] == 'TCGA':
                        tcga_code = f"{parts[0]}-{parts[1]}"
                
                if tcga_code:
                    tcga_codes_found.add(tcga_code)
                
                # Map TCGA code to cancer_type_id
                cancer_type_id = cancer_type_map.get(tcga_code) if tcga_code else None
                
                if cancer_type_id is None and tcga_code:
                    samples_with_null_cancer_type.append((sample_barcode, tcga_code))
                elif cancer_type_id is None and not tcga_code:
                    samples_with_null_cancer_type.append((sample_barcode, "NO-TCGA-CODE"))
                
                all_samples.append({
                    'sample_barcode': sample_barcode,
                    'sample_type': sample_type,
                    'cancer_type_id': cancer_type_id
                })
            
            print(f"  {cancer_name}: Processed {len(sample_df)} samples from sample_sheet.csv")
            
        except Exception as e:
            print(f"  ERROR reading sample_sheet.csv for {cancer_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\nFound {len(all_samples):,} total samples")
    
    # Debug information
    if samples_with_null_cancer_type:
        print(f"\nWARNING: {len(samples_with_null_cancer_type)} samples have NULL cancer_type_id")
        print(f"  Unique TCGA codes found in samples: {sorted(tcga_codes_found)}")
        print(f"  TCGA codes in cancer_type_map: {sorted(cancer_type_map.keys())}")
        
        # Check which TCGA codes are missing from the map
        missing_tcga_codes = tcga_codes_found - set(cancer_type_map.keys())
        if missing_tcga_codes:
            print(f"  MISSING TCGA codes (found in samples but not in map): {sorted(missing_tcga_codes)}")
            # Verify if they exist in database
            if missing_tcga_codes:
                cur.execute("SELECT tcga_code FROM cancer_types WHERE tcga_code IN %s", 
                           (tuple(missing_tcga_codes),))
                db_tcga_codes = {row['tcga_code'] for row in cur.fetchall()}
                if db_tcga_codes:
                    print(f"  Found {len(db_tcga_codes)} of these codes in database but not in map!")
                else:
                    print(f"  These codes are NOT in the database - they weren't inserted!")
        
        print(f"  First 5 examples of NULL cancer_type_id:")
        for barcode, tcga_code in samples_with_null_cancer_type[:5]:
            print(f"    - {barcode} -> TCGA code: {tcga_code} -> cancer_type_id: NULL")
    else:
        print(f"  All samples have cancer_type_id assigned")
    
    # Insert samples
    insert_query = """
        INSERT INTO samples (sample_barcode, sample_type, cancer_type_id)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            sample_type = VALUES(sample_type),
            cancer_type_id = VALUES(cancer_type_id)
    """
    
    records = [(s['sample_barcode'], s['sample_type'], s['cancer_type_id']) 
               for s in all_samples]
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        cur.executemany(insert_query, batch)
        conn.commit()
        print(f"  Inserted batch {i:,}–{i + len(batch):,}")
    
    # Fetch sample map
    cur.execute("SELECT id, sample_barcode FROM samples")
    sample_map = {row['sample_barcode']: row['id'] for row in cur.fetchall()}
    
    print(f"Populated {len(sample_map):,} samples")
    return sample_map


def populate_gene_expressions_table(conn, data_dir: str, cancer_names: List[str],
                                    gene_map: Dict[str, int], 
                                    sample_map: Dict[str, int]):
    """
    Load expression data from CSV files and populate gene_expressions table.
    """
    cur = conn.cursor()
    
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if not os.path.isdir(folder_path):
            print(f"WARNING: Folder not found: {folder_path}")
            continue
        
        print(f"\nProcessing {cancer_name}...")
        
        for prefix in ['tumor', 'normal']:
            process_expression_set(conn, folder_path, prefix, cancer_name, 
                                  gene_map, sample_map)


def process_expression_set(conn, folder_path: str, prefix: str, cancer_name: str,
                          gene_map: Dict[str, int], sample_map: Dict[str, int]):
    """
    Process one expression type (tumor or normal) and load into gene_expressions table.
    """
    print(f"  Processing {prefix.upper()} data...")
    cur = conn.cursor()
    
    # File paths
    tpm_path = os.path.join(folder_path, f"{prefix}_tpm.csv")
    fpkm_path = os.path.join(folder_path, f"{prefix}_fpkm.csv")
    fpkm_uq_path = os.path.join(folder_path, f"{prefix}_fpkm_uq.csv")
    
    # Check existence
    if not all(os.path.exists(p) for p in [tpm_path, fpkm_path, fpkm_uq_path]):
        print(f"  WARNING: Missing {prefix} files, skipping.")
        return
    
    # Load CSVs
    print("  Loading CSV files...")
    read_opts = dict(low_memory=False, dtype=str)
    tpm_df = pd.read_csv(tpm_path, **read_opts)
    fpkm_df = pd.read_csv(fpkm_path, **read_opts)
    fpkm_uq_df = pd.read_csv(fpkm_uq_path, **read_opts)
    
    # Drop gene_name and normalize gene_id
    for df in (tpm_df, fpkm_df, fpkm_uq_df):
        df.drop(columns=["gene_name"], errors="ignore", inplace=True)
        df.rename(columns={"gene_id": "ensembl_id"}, inplace=True)
    
    # Melt (wide=> long)
    print("  Melting dataframes...")
    tpm_long = tpm_df.melt(id_vars="ensembl_id", var_name="sample_barcode", value_name="tpm")
    fpkm_long = fpkm_df.melt(id_vars="ensembl_id", var_name="sample_barcode", value_name="fpkm")
    fpkm_uq_long = fpkm_uq_df.melt(id_vars="ensembl_id", var_name="sample_barcode", value_name="fpkm_uq")
    
    # Merge all three
    merged_df = tpm_long.merge(fpkm_long, on=["ensembl_id", "sample_barcode"])
    merged_df = merged_df.merge(fpkm_uq_long, on=["ensembl_id", "sample_barcode"])
    print(f"  Merged rows: {len(merged_df):,}")
    
    # Map gene_id and sample_id
    print("  Mapping IDs...")
    merged_df["gene_id"] = merged_df["ensembl_id"].map(gene_map)
    merged_df["sample_id"] = merged_df["sample_barcode"].map(sample_map)
    merged_df.dropna(subset=["gene_id", "sample_id"], inplace=True)
    
    # Keep only needed columns and convert to numeric
    merged_df = merged_df[["gene_id", "sample_id", "tpm", "fpkm", "fpkm_uq"]]
    merged_df[["tpm", "fpkm", "fpkm_uq"]] = merged_df[["tpm", "fpkm", "fpkm_uq"]].apply(
        pd.to_numeric, errors="coerce"
    )
    
    print(f"  Ready to insert {len(merged_df):,} records")
    
    # Write to temporary TSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as tmpfile:
        tmp_path = tmpfile.name
        merged_df.to_csv(tmp_path, sep="\t", header=False, index=False)
    
    # Try fast LOAD DATA LOCAL INFILE
    try:
        print("  Bulk loading using LOAD DATA LOCAL INFILE...")
        # Escape backslashes in path for SQL
        escaped_path = tmp_path.replace('\\', '\\\\')
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{escaped_path}'
        INTO TABLE gene_expressions
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\n'
        (gene_id, sample_id, tpm, fpkm, fpkm_uq)
        ON DUPLICATE KEY UPDATE
            tpm = VALUES(tpm),
            fpkm = VALUES(fpkm),
            fpkm_uq = VALUES(fpkm_uq)
        """
        cur.execute(load_sql)
        conn.commit()
        print(f"  Successfully loaded {len(merged_df):,} {prefix} records")
    
    except Exception as e:
        print(f"  ERROR: LOAD DATA failed: {e}")
        print("  Falling back to executemany()...")
        
        insert_query = """
            INSERT INTO gene_expressions (gene_id, sample_id, tpm, fpkm, fpkm_uq)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tpm = VALUES(tpm),
                fpkm = VALUES(fpkm),
                fpkm_uq = VALUES(fpkm_uq)
        """
        
        records = merged_df.values.tolist()
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            cur.executemany(insert_query, batch)
            conn.commit()
            print(f"  Inserted batch {i:,}–{i + len(batch):,}")
    
    finally:
        os.remove(tmp_path)
        print(f"  Cleaned up temp file")

def populate_depth_scores(conn, data_dir: str, cancer_names: List[str], 
                          sample_map: Dict[str, int]):
    """
    Calculate and populate DEPTH2 and DEPTH scores for all tumor samples.
    Processes each cancer site, loads expression data, applies log2 transformation,
    calculates scores, and inserts them into depth2_scores and depth_scores tables.
    """
    print("\n" + "=" * 60)
    print("STEP 6: Calculating and populating DEPTH2 and DEPTH scores")
    print("=" * 60)
    cur = conn.cursor()
    
    for cancer_name in cancer_names:
        folder_path = os.path.join(data_dir, cancer_name)
        if not os.path.isdir(folder_path):
            continue
        
        print(f"\nProcessing {cancer_name}...")
        
        # File paths for tumor data
        tpm_path = os.path.join(folder_path, "tumor_tpm.csv")
        fpkm_path = os.path.join(folder_path, "tumor_fpkm.csv")
        fpkm_uq_path = os.path.join(folder_path, "tumor_fpkm_uq.csv")
        
        # Check if compressed files exist
        if not os.path.exists(tpm_path):
            tpm_path = os.path.join(folder_path, "tumor_tpm.csv.gz")
        if not os.path.exists(fpkm_path):
            fpkm_path = os.path.join(folder_path, "tumor_fpkm.csv.gz")
        if not os.path.exists(fpkm_uq_path):
            fpkm_uq_path = os.path.join(folder_path, "tumor_fpkm_uq.csv.gz")
        
        # Check if at least TPM file exists
        if not os.path.exists(tpm_path):
            print(f"  WARNING: Tumor TPM file not found for {cancer_name}, skipping")
            continue
        
        try:
            # Load expression data
            print(f"  Loading expression data...")
            tpm_df = pd.read_csv(tpm_path, dtype=str, low_memory=False)
            sample_columns = [col for col in tpm_df.columns if col not in ['gene_id', 'gene_name']]
            
            if not sample_columns:
                print(f"  WARNING: No sample columns found in {cancer_name}, skipping")
                continue
            
            print(f"  Found {len(tpm_df)} genes and {len(sample_columns)} tumor samples")
            
            # Prepare expression matrices (all genes, all samples)
            expr_matrix_tpm = tpm_df.set_index('gene_id')[sample_columns].copy()
            expr_matrix_tpm = expr_matrix_tpm.apply(pd.to_numeric, errors='coerce')
            
            expr_matrix_fpkm = None
            expr_matrix_fpkm_uq = None
            
            if os.path.exists(fpkm_path):
                fpkm_df = pd.read_csv(fpkm_path, dtype=str, low_memory=False)
                expr_matrix_fpkm = fpkm_df.set_index('gene_id')[sample_columns].copy()
                expr_matrix_fpkm = expr_matrix_fpkm.apply(pd.to_numeric, errors='coerce')
            
            if os.path.exists(fpkm_uq_path):
                fpkm_uq_df = pd.read_csv(fpkm_uq_path, dtype=str, low_memory=False)
                expr_matrix_fpkm_uq = fpkm_uq_df.set_index('gene_id')[sample_columns].copy()
                expr_matrix_fpkm_uq = expr_matrix_fpkm_uq.apply(pd.to_numeric, errors='coerce')
            
            # Apply log2 transformation
            print(f"  Applying log2 transformation...")
            expr_matrix_tpm_log2 = np.log2(expr_matrix_tpm + 1)
            expr_matrix_fpkm_log2 = np.log2(expr_matrix_fpkm + 1) if expr_matrix_fpkm is not None else None
            expr_matrix_fpkm_uq_log2 = np.log2(expr_matrix_fpkm_uq + 1) if expr_matrix_fpkm_uq is not None else None
            
            # Calculate DEPTH2 scores
            print(f"  Calculating DEPTH2 scores...")
            depth2_scores_tpm = depth2_calculation(expr_matrix_tpm_log2)
            depth2_scores_fpkm = depth2_calculation(expr_matrix_fpkm_log2) if expr_matrix_fpkm_log2 is not None else None
            depth2_scores_fpkm_uq = depth2_calculation(expr_matrix_fpkm_uq_log2) if expr_matrix_fpkm_uq_log2 is not None else None
            
            # Calculate DEPTH scores
            print(f"  Calculating DEPTH scores...")
            depth_scores_tpm = depth_calculation(expr_matrix_tpm_log2)
            depth_scores_fpkm = depth_calculation(expr_matrix_fpkm_log2) if expr_matrix_fpkm_log2 is not None else None
            depth_scores_fpkm_uq = depth_calculation(expr_matrix_fpkm_uq_log2) if expr_matrix_fpkm_uq_log2 is not None else None
            
            # Insert DEPTH2 scores
            print(f"  Inserting DEPTH2 scores...")
            insert_depth2_query = """
                INSERT INTO depth2_scores (sample_id, tpm, fpkm, fpkm_uq)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tpm = VALUES(tpm),
                    fpkm = VALUES(fpkm),
                    fpkm_uq = VALUES(fpkm_uq)
            """
            
            depth2_records = []
            for sample_barcode in sample_columns:
                sample_id = sample_map.get(sample_barcode)
                if sample_id:
                    tpm_val = depth2_scores_tpm.get(sample_barcode)
                    fpkm_val = depth2_scores_fpkm.get(sample_barcode) if depth2_scores_fpkm is not None else None
                    fpkm_uq_val = depth2_scores_fpkm_uq.get(sample_barcode) if depth2_scores_fpkm_uq is not None else None
                    depth2_records.append((
                        int(sample_id),
                        float(tpm_val) if pd.notna(tpm_val) else None,
                        float(fpkm_val) if fpkm_val is not None and pd.notna(fpkm_val) else None,
                        float(fpkm_uq_val) if fpkm_uq_val is not None and pd.notna(fpkm_uq_val) else None
                    ))
            
            if depth2_records:
                cur.executemany(insert_depth2_query, depth2_records)
                conn.commit()
                print(f"    Inserted {len(depth2_records)} DEPTH2 score records")
            
            # Insert DEPTH scores
            print(f"  Inserting DEPTH scores...")
            insert_depth_query = """
                INSERT INTO depth_scores (sample_id, tpm, fpkm, fpkm_uq)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tpm = VALUES(tpm),
                    fpkm = VALUES(fpkm),
                    fpkm_uq = VALUES(fpkm_uq)
            """
            
            depth_records = []
            for sample_barcode in sample_columns:
                sample_id = sample_map.get(sample_barcode)
                if sample_id:
                    tpm_val = depth_scores_tpm.get(sample_barcode)
                    fpkm_val = depth_scores_fpkm.get(sample_barcode) if depth_scores_fpkm is not None else None
                    fpkm_uq_val = depth_scores_fpkm_uq.get(sample_barcode) if depth_scores_fpkm_uq is not None else None
                    depth_records.append((
                        int(sample_id),
                        float(tpm_val) if pd.notna(tpm_val) else None,
                        float(fpkm_val) if fpkm_val is not None and pd.notna(fpkm_val) else None,
                        float(fpkm_uq_val) if fpkm_uq_val is not None and pd.notna(fpkm_uq_val) else None
                    ))
            
            if depth_records:
                cur.executemany(insert_depth_query, depth_records)
                conn.commit()
                print(f"    Inserted {len(depth_records)} DEPTH score records")
            
            print(f"  Completed {cancer_name}: {len(sample_columns)} samples processed")
            
        except Exception as e:
            print(f"  ERROR processing {cancer_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 60)
    print("DEPTH2 and DEPTH scores populated successfully!")
    print("=" * 60)

def main():
    """Main function to populate all tables."""
    print("=" * 60)
    print("Starting DB Population Pipeline")
    print("=" * 60)
    
    # Automatically detect cancer sites from data directory
    print(f"\nScanning data directory: {DATA_DIR}")
    cancer_names = get_cancer_sites_from_directory(DATA_DIR)
    
    if not cancer_names:
        print("ERROR: No cancer sites found in data directory.")
        print("Please ensure CSV files are present in subdirectories of:", DATA_DIR)
        return
    
    print(f"Found {len(cancer_names)} cancer site(s): {', '.join(cancer_names)}")
    print("=" * 60)
    
    conn = get_connection()
    
    try:
        # Step 1: Populate genes table
        gene_map = populate_genes_table(conn, DATA_DIR, cancer_names)
        
        # Step 2: Populate sites table
        site_map = populate_sites_table(conn, DATA_DIR, cancer_names)
        
        # Step 3: Populate cancer_types table
        cancer_type_map = populate_cancer_types_table(conn, DATA_DIR, cancer_names, site_map)
        
        # Step 4: Populate samples table
        sample_map = populate_samples_table(conn, DATA_DIR, cancer_names, 
                                           site_map, cancer_type_map)
        
        # Step 5: Populate gene_expressions table
        print("\nPopulating gene_expressions table...")
        populate_gene_expressions_table(conn, DATA_DIR, cancer_names, 
                                       gene_map, sample_map)
        
        # Step 6: Calculate and populate DEPTH2 and DEPTH scores
        populate_depth_scores(conn, DATA_DIR, cancer_names, sample_map)
        
        print("\n" + "=" * 60)
        print("All tables populated successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def example_populate_single_file():
    """
    EXAMPLE FUNCTION: Demonstrates how to populate a single CSV file into the database.
    
    This example uses backend/data/raw/Thymus/tumor_tpm.csv as a reference.
    
    Usage:
        python populate_db.py --example
    """
    print("=" * 60)
    print("EXAMPLE: Populating Single File (Thymus/tumor_tpm.csv)")
    print("=" * 60)
    
    # Example file path - try different possible folder names
    possible_folders = ["Thymus"]
    example_file = None
    for folder in possible_folders:
        test_path = os.path.join(DATA_DIR, folder, "tumor_tpm.csv")
        if os.path.exists(test_path):
            example_file = test_path
            break
    
    if example_file is None:
        # Try to find any folder with tumor_tpm.csv
        for item in os.listdir(DATA_DIR):
            item_path = os.path.join(DATA_DIR, item)
            if os.path.isdir(item_path):
                test_path = os.path.join(item_path, "tumor_tpm.csv")
                if os.path.exists(test_path):
                    example_file = test_path
                    break
    
    if example_file is None:
        print(f"ERROR: Example file not found in any expected location.")
        print(f"Searched in: {DATA_DIR}")
        print("Please ensure tumor_tpm.csv exists in a subdirectory.")
        return
    
    print(f"\nExample file: {example_file}")
    
    # Extract folder name from path
    folder_name = os.path.basename(os.path.dirname(example_file))
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Step 1: Load the CSV file
        print("\nStep 1: Loading CSV file...")
        df = pd.read_csv(example_file, dtype=str, low_memory=False)
        print(f"  Loaded {len(df)} genes and {len(df.columns) - 2} samples")
        print(f"  Columns: gene_id, gene_name, and {len(df.columns) - 2} sample columns")
        
        # Step 2: Extract genes
        print("\nStep 2: Extracting unique genes...")
        genes_df = df[['gene_id', 'gene_name']].drop_duplicates()
        print(f"  Found {len(genes_df)} unique genes")
        print(f"  Example genes:")
        for idx, row in genes_df.head(3).iterrows():
            print(f"    - {row['gene_id']} ({row['gene_name']})")
        
        # Step 3: Insert genes into database 
        print("\nStep 3: Inserting genes into database...")
        insert_genes_query = """
            INSERT INTO genes (ensembl_id, gene_symbol)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE gene_symbol = VALUES(gene_symbol)
        """
        gene_records = [(row['gene_id'], row['gene_name']) for _, row in genes_df.iterrows()]
        cur.executemany(insert_genes_query, gene_records)
        conn.commit()
        print(f"  Inserted {len(gene_records)} genes ")
        
        # Step 4: Get gene map
        cur.execute("SELECT id, ensembl_id FROM genes WHERE ensembl_id IN %s", 
                   (tuple(genes_df['gene_id'].tolist()),))
        gene_map = {row['ensembl_id']: row['id'] for row in cur.fetchall()}
        print(f"  Retrieved {len(gene_map)} gene IDs from database")
        
        # Step 5: Extract sample barcodes
        print("\nStep 5: Extracting sample barcodes...")
        sample_columns = [col for col in df.columns if col not in ['gene_id', 'gene_name']]
        print(f"  Found {len(sample_columns)} sample barcodes")
        print(f"  Example samples: {', '.join(sample_columns[:5])}")
        
        # Step 6: Insert site (if not exists)
        print("\nStep 6: Ensuring site exists in database...")
        # Use folder name directly as site name (must match preprocess.py format)
        site_name = folder_name
        
        cur.execute("INSERT INTO sites (name) VALUES (%s) ON DUPLICATE KEY UPDATE name = VALUES(name)", 
                   (site_name,))
        conn.commit()
        cur.execute("SELECT id FROM sites WHERE name = %s", (site_name,))
        result = cur.fetchone()
        site_id = result['id'] if result else None
        print(f"  Site ID for '{site_name}': {site_id}")
        
        # Step 7: Populate cancer_types using sample_sheet.csv
        print("\nStep 7: Populating cancer_types table using sample_sheet.csv...")
        folder_path = os.path.dirname(example_file)
        sample_sheet_path = os.path.join(folder_path, "sample_sheet.csv")
        
        cancer_type_map = {}
        if os.path.exists(sample_sheet_path):
            try:
                sample_sheet_df = pd.read_csv(sample_sheet_path)
                print(f"  Loaded sample_sheet.csv: {len(sample_sheet_df)} rows")
                
                if 'tcga_code' in sample_sheet_df.columns:
                    # Extract unique TCGA codes
                    unique_tcga_codes = sample_sheet_df['tcga_code'].dropna().unique()
                    print(f"  Found {len(unique_tcga_codes)} unique TCGA codes")
                    
                    # Insert cancer types
                    insert_cancer_types_query = """
                        INSERT INTO cancer_types (tcga_code, site_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE site_id = VALUES(site_id)
                    """
                    cancer_type_records = [(str(tcga_code).strip(), site_id) 
                                         for tcga_code in unique_tcga_codes 
                                         if pd.notna(tcga_code) and str(tcga_code).strip()]
                    
                    if cancer_type_records:
                        cur.executemany(insert_cancer_types_query, cancer_type_records)
                        conn.commit()
                        print(f"  Inserted {len(cancer_type_records)} cancer types")
                    
                    # Fetch cancer_type map
                    cur.execute("SELECT id, tcga_code FROM cancer_types WHERE tcga_code IN %s",
                               (tuple([str(tc).strip() for tc in unique_tcga_codes if pd.notna(tc)]),))
                    cancer_type_map = {row['tcga_code']: row['id'] for row in cur.fetchall()}
                    print(f"  Retrieved {len(cancer_type_map)} cancer type IDs")
                else:
                    print(f"  WARNING: 'tcga_code' column not found in sample_sheet.csv")
                    print(f"  Available columns: {', '.join(sample_sheet_df.columns)}")
            except Exception as e:
                print(f"  ERROR reading sample_sheet.csv: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"  WARNING: sample_sheet.csv not found at {sample_sheet_path}")
            print(f"  Will proceed without cancer_type_id mapping")
        
        # Step 8: Insert samples using sample_sheet.csv
        print("\nStep 8: Inserting sample metadata using sample_sheet.csv...")
        
        if os.path.exists(sample_sheet_path):
            try:
                sample_sheet_df = pd.read_csv(sample_sheet_path)
                
                # Prepare sample records with cancer_type_id
                sample_records = []
                for _, row in sample_sheet_df.iterrows():
                    sample_barcode = str(row['sample_barcode']).strip()
                    sample_type_raw = str(row['sample_type']).strip()
                    
                    # Normalize sample_type to "tumor" or "normal"
                    sample_type = normalize_sample_type(sample_type_raw)
                    
                    # Get TCGA code and map to cancer_type_id
                    tcga_code = None
                    if 'tcga_code' in sample_sheet_df.columns:
                        tcga_code_val = row.get('tcga_code')
                        if pd.notna(tcga_code_val):
                            tcga_code = str(tcga_code_val).strip()
                    
                    # Fallback: extract from sample_barcode
                    if not tcga_code:
                        parts = sample_barcode.split('-')
                        if len(parts) >= 2 and parts[0] == 'TCGA':
                            tcga_code = f"{parts[0]}-{parts[1]}"
                    
                    cancer_type_id = cancer_type_map.get(tcga_code) if tcga_code else None
                    
                    sample_records.append((sample_barcode, sample_type, cancer_type_id))
                
                insert_samples_query = """
                    INSERT INTO samples (sample_barcode, sample_type, cancer_type_id)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        sample_type = VALUES(sample_type),
                        cancer_type_id = VALUES(cancer_type_id)
                """
                cur.executemany(insert_samples_query, sample_records)
                conn.commit()
                print(f"  Inserted {len(sample_records)} sample records from sample_sheet.csv")
                
            except Exception as e:
                print(f"  ERROR processing sample_sheet.csv: {e}")
                import traceback
                traceback.print_exc()
                # Fallback: insert without cancer_type_id
                insert_samples_query = """
                    INSERT INTO samples (sample_barcode, sample_type)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE sample_type = VALUES(sample_type)
                """
                sample_records = [(barcode, 'tumor') for barcode in sample_columns]
                cur.executemany(insert_samples_query, sample_records)
                conn.commit()
                print(f"  Fallback: Inserted {len(sample_records)} sample records without cancer_type_id")
        else:
            # Fallback: insert without sample sheet
            insert_samples_query = """
                INSERT INTO samples (sample_barcode, sample_type)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE sample_type = VALUES(sample_type)
            """
            sample_records = [(barcode, 'tumor') for barcode in sample_columns]
            cur.executemany(insert_samples_query, sample_records)
            conn.commit()
            print(f"  Inserted {len(sample_records)} sample records (no sample_sheet.csv found)")
        
        # Step 9: Get sample map (for all samples)
        cur.execute("SELECT id, sample_barcode FROM samples WHERE sample_barcode IN %s",
                   (tuple(sample_columns),))
        sample_map = {row['sample_barcode']: row['id'] for row in cur.fetchall()}
        print(f"  Retrieved {len(sample_map)} sample IDs from database")
        
        # Define example samples for expression data insertion (first 5)
        example_samples = sample_columns[:5]
        print(f"  Will insert expression data for {len(example_samples)} example samples")
        
        # Step 10: Load FPKM and FPKM-UQ data
        print("\nStep 9: Loading FPKM and FPKM-UQ data...")
        folder_path = os.path.dirname(example_file)
        fpkm_path = os.path.join(folder_path, "tumor_fpkm.csv")
        fpkm_uq_path = os.path.join(folder_path, "tumor_fpkm_uq.csv")
        
        fpkm_df = None
        fpkm_uq_df = None
        
        if os.path.exists(fpkm_path):
            fpkm_df = pd.read_csv(fpkm_path, dtype=str, low_memory=False)
            print(f"  Loaded FPKM data: {len(fpkm_df)} genes")
        else:
            print(f"  WARNING: FPKM file not found: {fpkm_path}")
        
        if os.path.exists(fpkm_uq_path):
            fpkm_uq_df = pd.read_csv(fpkm_uq_path, dtype=str, low_memory=False)
            print(f"  Loaded FPKM-UQ data: {len(fpkm_uq_df)} genes")
        else:
            print(f"  WARNING: FPKM-UQ file not found: {fpkm_uq_path}")
        
        # Step 11: Melt dataframes (wide to long format) - first 5 samples
        print("\nStep 10: Converting wide format to long format...")
        tpm_long = df.head(100).melt(  # Use first 100 genes as example
            id_vars=['gene_id', 'gene_name'],
            value_vars=example_samples,  # Use first 5 samples as example
            var_name='sample_barcode',
            value_name='tpm'
        )
        
        fpkm_long = None
        fpkm_uq_long = None
        
        if fpkm_df is not None:
            fpkm_long = fpkm_df.head(100).melt(  # Use first 100 genes as example
                id_vars=['gene_id', 'gene_name'],
                value_vars=example_samples,  # Use first 5 samples as example
                var_name='sample_barcode',
                value_name='fpkm'
            )
        
        if fpkm_uq_df is not None:
            fpkm_uq_long = fpkm_uq_df.head(100).melt(  # Use first 100 genes as example
                id_vars=['gene_id', 'gene_name'],
                value_vars=example_samples,  # Use first 5 samples as example
                var_name='sample_barcode',
                value_name='fpkm_uq'
            )
        
        # Merge all three dataframes
        merged_df = tpm_long.copy()
        if fpkm_long is not None:
            merged_df = merged_df.merge(fpkm_long[['gene_id', 'sample_barcode', 'fpkm']], 
                                      on=['gene_id', 'sample_barcode'], how='left')
        else:
            merged_df['fpkm'] = None
        
        if fpkm_uq_long is not None:
            merged_df = merged_df.merge(fpkm_uq_long[['gene_id', 'sample_barcode', 'fpkm_uq']], 
                                      on=['gene_id', 'sample_barcode'], how='left')
        else:
            merged_df['fpkm_uq'] = None
        
        print(f"  Converted to long format: {len(merged_df)} rows")
        print(f"  Example rows:")
        for idx, row in merged_df.head(3).iterrows():
            print(f"    - Gene: {row['gene_id']}, Sample: {row['sample_barcode']}, TPM: {row['tpm']}")
        
        # Step 12: Map IDs and prepare for insertion
        print("\nStep 11: Mapping gene and sample IDs...")
        merged_df['gene_id_db'] = merged_df['gene_id'].map(gene_map)
        merged_df['sample_id_db'] = merged_df['sample_barcode'].map(sample_map)
        merged_df = merged_df.dropna(subset=['gene_id_db', 'sample_id_db'])
        
        # Convert to numeric
        merged_df['tpm'] = pd.to_numeric(merged_df['tpm'], errors='coerce')
        merged_df['fpkm'] = pd.to_numeric(merged_df['fpkm'], errors='coerce')
        merged_df['fpkm_uq'] = pd.to_numeric(merged_df['fpkm_uq'], errors='coerce')
        
        print(f"  Mapped {len(merged_df)} records ready for insertion")
        
        # Step 13: Insert expression data
        print("\nStep 12: Inserting expression data...")
        insert_expr_query = """
            INSERT INTO gene_expressions (gene_id, sample_id, tpm, fpkm, fpkm_uq)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                tpm = VALUES(tpm),
                fpkm = VALUES(fpkm),
                fpkm_uq = VALUES(fpkm_uq)
        """
        expr_records = [
            (int(row['gene_id_db']), int(row['sample_id_db']), 
             float(row['tpm']) if pd.notna(row['tpm']) else None,
             float(row['fpkm']) if pd.notna(row['fpkm']) else None,
             float(row['fpkm_uq']) if pd.notna(row['fpkm_uq']) else None)
            for _, row in merged_df.iterrows()
        ]
        
        # Insert in batches
        for i in range(0, len(expr_records), BATCH_SIZE):
            batch = expr_records[i:i + BATCH_SIZE]
            cur.executemany(insert_expr_query, batch)
            conn.commit()
            print(f"  Inserted batch {i:,}–{i + len(batch):,}")
        
        print(f"  Total inserted: {len(expr_records)} expression records")
        
        # Step 14: Calculate and populate DEPTH2 and DEPTH scores
        print("\nStep 13: Calculating DEPTH2 and DEPTH scores...")
        
        # Prepare expression matrices (all genes, all samples) for score calculation
        # Set gene_id as index and keep only sample columns
        expr_matrix_tpm = df.set_index('gene_id')[sample_columns].copy()
        expr_matrix_tpm = expr_matrix_tpm.apply(pd.to_numeric, errors='coerce')
        print(f"  Prepared TPM matrix: {expr_matrix_tpm.shape[0]} genes x {expr_matrix_tpm.shape[1]} samples")
        
        expr_matrix_fpkm = None
        expr_matrix_fpkm_uq = None
        
        if fpkm_df is not None:
            expr_matrix_fpkm = fpkm_df.set_index('gene_id')[sample_columns].copy()
            expr_matrix_fpkm = expr_matrix_fpkm.apply(pd.to_numeric, errors='coerce')
            print(f"  Prepared FPKM matrix: {expr_matrix_fpkm.shape[0]} genes x {expr_matrix_fpkm.shape[1]} samples")
        
        if fpkm_uq_df is not None:
            expr_matrix_fpkm_uq = fpkm_uq_df.set_index('gene_id')[sample_columns].copy()
            expr_matrix_fpkm_uq = expr_matrix_fpkm_uq.apply(pd.to_numeric, errors='coerce')
            print(f"  Prepared FPKM-UQ matrix: {expr_matrix_fpkm_uq.shape[0]} genes x {expr_matrix_fpkm_uq.shape[1]} samples")
        
        # Apply log2 transformation (add pseudocount of 1 to avoid log(0))
        print("  Applying log2 transformation...")
        expr_matrix_tpm_log2 = np.log2(expr_matrix_tpm + 1)
        expr_matrix_fpkm_log2 = np.log2(expr_matrix_fpkm + 1) if expr_matrix_fpkm is not None else None
        expr_matrix_fpkm_uq_log2 = np.log2(expr_matrix_fpkm_uq + 1) if expr_matrix_fpkm_uq is not None else None
        
        # Calculate DEPTH2 scores on log2-transformed data
        print("  Calculating DEPTH2 scores...")
        depth2_scores_tpm = depth2_calculation(expr_matrix_tpm_log2)
        depth2_scores_fpkm = depth2_calculation(expr_matrix_fpkm_log2) if expr_matrix_fpkm_log2 is not None else None
        depth2_scores_fpkm_uq = depth2_calculation(expr_matrix_fpkm_uq_log2) if expr_matrix_fpkm_uq_log2 is not None else None
        
        # Calculate DEPTH scores on log2-transformed data (using tumor data only, no normal reference)
        print("  Calculating DEPTH scores...")
        depth_scores_tpm = depth_calculation(expr_matrix_tpm_log2)
        depth_scores_fpkm = depth_calculation(expr_matrix_fpkm_log2) if expr_matrix_fpkm_log2 is not None else None
        depth_scores_fpkm_uq = depth_calculation(expr_matrix_fpkm_uq_log2) if expr_matrix_fpkm_uq_log2 is not None else None
        
        print(f"  Calculated scores for {len(depth2_scores_tpm)} samples")
        
        # Insert DEPTH2 scores
        print("\nStep 14: Inserting DEPTH2 scores...")
        insert_depth2_query = """
            INSERT INTO depth2_scores (sample_id, tpm, fpkm, fpkm_uq)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tpm = VALUES(tpm),
                fpkm = VALUES(fpkm),
                fpkm_uq = VALUES(fpkm_uq)
        """
        
        depth2_records = []
        for sample_barcode in sample_columns:  # Insert scores for all samples
            sample_id = sample_map.get(sample_barcode)
            if sample_id:
                tpm_val = depth2_scores_tpm.get(sample_barcode)
                fpkm_val = depth2_scores_fpkm.get(sample_barcode) if depth2_scores_fpkm is not None else None
                fpkm_uq_val = depth2_scores_fpkm_uq.get(sample_barcode) if depth2_scores_fpkm_uq is not None else None
                depth2_records.append((
                    int(sample_id),
                    float(tpm_val) if pd.notna(tpm_val) else None,
                    float(fpkm_val) if fpkm_val is not None and pd.notna(fpkm_val) else None,
                    float(fpkm_uq_val) if fpkm_uq_val is not None and pd.notna(fpkm_uq_val) else None
                ))
        
        cur.executemany(insert_depth2_query, depth2_records)
        conn.commit()
        print(f"  Inserted {len(depth2_records)} DEPTH2 score records (for all {len(sample_columns)} samples)")
        
        # Insert DEPTH scores
        print("\nStep 15: Inserting DEPTH scores...")
        insert_depth_query = """
            INSERT INTO depth_scores (sample_id, tpm, fpkm, fpkm_uq)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tpm = VALUES(tpm),
                fpkm = VALUES(fpkm),
                fpkm_uq = VALUES(fpkm_uq)
        """
        
        depth_records = []
        for sample_barcode in sample_columns:  # Insert scores for all samples
            sample_id = sample_map.get(sample_barcode)
            if sample_id:
                tpm_val = depth_scores_tpm.get(sample_barcode)
                fpkm_val = depth_scores_fpkm.get(sample_barcode) if depth_scores_fpkm is not None else None
                fpkm_uq_val = depth_scores_fpkm_uq.get(sample_barcode) if depth_scores_fpkm_uq is not None else None
                depth_records.append((
                    int(sample_id),
                    float(tpm_val) if pd.notna(tpm_val) else None,
                    float(fpkm_val) if fpkm_val is not None and pd.notna(fpkm_val) else None,
                    float(fpkm_uq_val) if fpkm_uq_val is not None and pd.notna(fpkm_uq_val) else None
                ))
        
        cur.executemany(insert_depth_query, depth_records)
        conn.commit()
        print(f"  Inserted {len(depth_records)} DEPTH score records (for all {len(sample_columns)} samples)")

    except Exception as e:
        print(f"\nERROR in example: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--example":
        example_populate_single_file()
    else:
        main()

