import sys

sys.dont_write_bytecode = True
from fastapi import FastAPI, Body, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
import pymysql
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional, Set
from cv import cv_calculation
from std import std_calculation
from MAD import mad_calculation
from DEPTH2 import depth2_calculation
from DEPTH_ITH import depth_calculation
from cv_2 import cv2_calculation
from mean import mean_calculation
from get_pathway_genes import fetch_kegg_genes, fetch_go_genes
import re
import pandas as pd
import numpy as np
import math
import scipy.stats as stats
import requests
import logging
from db_conn import get_connection
import io
from pydantic import BaseModel

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI setup
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === METRIC FUNCTIONS ===
metric_funcs_gene = {
    "cv": cv_calculation,
    "cv_squared": cv2_calculation,
    "std": std_calculation,
    "mad": mad_calculation,
    "mean": mean_calculation,
}

metric_funcs_TH = {
    "DEPTH2": depth2_calculation,
    "DEPTH": depth_calculation,
}


def sanitize_floats(obj):
    """Recursively replace NaN, inf, and -inf with None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: sanitize_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_floats(v) for v in obj]
    elif isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    return obj

class GeneRequest(BaseModel):
    genes: List[str]


def zscore_series(values: pd.Series) -> pd.Series:
    """Return Z-score (standardised) series. NaN → NaN."""
    if len(values) == 0:
        return values
    mean = values.mean()
    std = values.std(ddof=1)
    if std == 0:
        return pd.Series([0.0] * len(values), index=values.index)
    return (values - mean) / std

def compute_metrics(values: pd.Series):
    """Compute mean, std, mad, cv, and cv² for a numeric series."""
    if len(values) == 0:
        return None
    mean = values.mean()
    std = values.std(ddof=1)
    mad = (values - mean).abs().mean()
    cv = (std / mean) * 100 if mean != 0 else np.nan
    cv2 = cv**2 if not np.isnan(cv) else np.nan
    return {"mean": mean, "std": std, "mad": mad, "cv": cv, "cv_squared": cv2}

# site selector
@app.get("/api/sites")
def get_sites():
    """Fetch all available cancer sites."""
    conn = get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    try:
        cur.execute("SELECT id, name FROM sites ORDER BY name ASC")
        sites = cur.fetchall()
        return JSONResponse({"sites": sites})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# gene selector
@app.get("/api/genes")
def get_genes():
    """Fetch all available gene symbols."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT gene_symbol FROM genes WHERE gene_symbol IS NOT NULL ORDER BY gene_symbol")
        return {"genes": [row["gene_symbol"] for row in cur.fetchall()]}

@app.get("/api/gene_names")
def get_gene_names(ensembl_ids: str):
    """
    ?ensembl_ids=ENSG000001,ENSG000002
    → {"ENSG000001": "BRCA1", "ENSG000002": "TP53", ...}
    """
    ids = ensembl_ids.split(",")
    placeholders = ",".join(["%s"] * len(ids))
    sql = f"SELECT ensembl_id, gene_symbol FROM genes WHERE ensembl_id IN ({placeholders})"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, ids)
        return dict(cur.fetchall())
    
@app.get("/api/ensembl/{gene}")
def get_ensembl_id(gene: str):
    """Fetch Ensembl ID for a specific gene symbol."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT ensembl_id FROM genes WHERE gene_symbol = %s LIMIT 1",
            (gene,),
        )
        row = cur.fetchone()
        if row:
            return {"ensembl_id": row["ensembl_id"]}
        return {"ensembl_id": None}

@app.get("/api/gene_IDs")
def get_all_genes():
    """Fetch all genes with their Ensembl IDs."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT gene_symbol, ensembl_id FROM genes WHERE ensembl_id IS NOT NULL ORDER BY gene_symbol")
        rows = cur.fetchall()

    gene_map: dict[str, list[str]] = {}
    for row in rows:
        gene = row["gene_symbol"]
        ens_id = row["ensembl_id"]
        if gene not in gene_map:
            gene_map[gene] = []
        gene_map[gene].append(ens_id)

    return {"genes": gene_map}


# Top noisy genes dashboard
@app.get("/api/top-noisy-genes")
def get_top_noisy_genes(
    norm: str = Query(..., description="tpm, fpkm, or fpkm_uq"),
    top_n: int = Query(500, ge=0, le=500),
    sites: Optional[str] = Query(None, description="Comma-separated site names"),
    tcga_codes: Optional[str] = Query(None, description="Comma-separated TCGA codes e.g. LUAD,LUSC")
):
    if norm not in ["tpm", "fpkm", "fpkm_uq"]:
        raise HTTPException(400, "Invalid normalization")

    with get_connection() as conn:
        cur = conn.cursor(pymysql.cursors.DictCursor)

        # === STEP 1: Check if noisy_gene_cache table exists ===
        cur.execute("""
            SELECT COUNT(*) AS table_exists
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = 'noisy_gene_cache'
        """)
        table_exists = cur.fetchone()['table_exists'] == 1

        if not table_exists:
            sql_dump_path = "./noisy_gene_cache.sql"                
            if not os.path.exists(sql_dump_path):
                raise HTTPException(
                    status_code=500,
                    detail="noisy_gene_cache table is missing and noisy_gene_cache.sql dump file not found."
                )

            print("noisy_gene_cache table not found. Creating and populating from SQL dump...")
            
            # Read and execute the SQL dump
            with open(sql_dump_path, 'r', encoding='utf-8') as f:
                sql_commands = f.read()

            # Split by semicolon and filter out empty statements
            statements = [stmt.strip() for stmt in sql_commands.split(';') if stmt.strip()]

            try:
                for statement in statements:
                    cur.execute(statement)
                conn.commit()
                print("noisy_gene_cache table created and populated successfully.")
            except Exception as e:
                conn.rollback()
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create noisy_gene_cache table from dump: {str(e)}"
                )

        # === STEP 2: Proceed with original logic (table now guaranteed to exist) ===
        filter_sql = ""
        params = [norm]  # for tumor query

        # CASE 1: If user gives cancer types
        if tcga_codes:
            codes = [c.strip() for c in tcga_codes.split(",") if c.strip()]
            placeholder = ",".join(["%s"] * len(codes))
            filter_sql += (
                f" AND n.cancer_type_id IN (SELECT id FROM cancer_types "
                f"WHERE tcga_code IN ({placeholder}))"
            )
            params.extend(codes)

        # CASE 2: If user gives sites
        elif sites:
            site_list = [s.strip() for s in sites.split(",") if s.strip()]
            placeholder = ",".join(["%s"] * len(site_list))
            filter_sql += f" AND s.name IN ({placeholder})"
            params.extend(site_list)

        # CASE 3: No filters provided → get common noisy genes across ALL sites
        else:
            # --- Find all site IDs available ---
            cur.execute(
                "SELECT DISTINCT site_id FROM noisy_gene_cache WHERE norm = %s AND sample_type='tumor'",
                (norm,)
            )
            all_sites = [row["site_id"] for row in cur.fetchall()]

            if not all_sites:
                return {"genes": [], "count": 0}

            placeholder = ",".join(["%s"] * len(all_sites))

            # Override filter: restrict to all sites
            filter_sql += f" AND n.site_id IN ({placeholder})"
            params.extend(all_sites)

            # --- Use aggregated CV across sites ---
            # Replace the tumor_query later with site-aggregated ranking
            aggregated_mode = True

        # ---- Query #1: Tumor CV ----
        tumor_query = f"""
            SELECT g.gene_symbol,
                   n.gene_id,
                   n.cv AS cv_tumor,
                   n.site_id,
                   s.name AS site_name
            FROM noisy_gene_cache n
            JOIN genes g ON n.gene_id = g.id
            JOIN sites s ON n.site_id = s.id
            WHERE n.norm = %s AND n.sample_type = 'tumor'
            {filter_sql}
            ORDER BY n.cv DESC
            LIMIT %s
        """
        params_with_limit = params + [top_n]

        cur.execute(tumor_query, params_with_limit)
        tumor_rows = cur.fetchall()

        # ---- Prepare for normal lookup ----
        if not tumor_rows:
            return {"genes": [], "count": 0}

        # ---- Query #2: Normal CV (same filters as tumor for consistency) ----
        normal_query = f"""
            SELECT g.gene_symbol,
                   n.gene_id,
                   n.cv AS cv_normal,
                   n.site_id,
                   s.name AS site_name
            FROM noisy_gene_cache n
            JOIN genes g ON n.gene_id = g.id
            JOIN sites s ON n.site_id = s.id
            WHERE n.norm = %s AND n.sample_type = 'normal'
            {filter_sql}
            ORDER BY n.cv DESC
            LIMIT %s
        """

        cur.execute(normal_query, params_with_limit)
        normal_rows = cur.fetchall()

        # Build lookup dict: (gene_id, site_id) -> cv_normal
        normal_lookup = {
            (row["gene_id"], row["site_id"]): row["cv_normal"]
            for row in normal_rows
        }

        # ---- Merge tumor + normal ----
        merged = []
        for r in tumor_rows:
            key = (r["gene_id"], r["site_id"])
            cv_normal = normal_lookup.get(key)

            merged.append({
                "gene_symbol": r["gene_symbol"],
                "cv_tumor": round(float(r["cv_tumor"]), 6) * 100,
                "cv_normal": round(float(cv_normal), 6) * 100 if cv_normal is not None else None,
                "site_name": r["site_name"]
            })

        return {"genes": merged, "count": len(merged)}

# gene analysis
@app.get("/api/gene_noise")
def get_gene_noise(
    cancer_site: list[str] = Query(..., description="One or more cancer sites"),
    cancer_type: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
    gene_ids: list[str] = Query(..., description="One or more gene symbols or Ensembl IDs")
):
    conn = get_connection()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    results = {}
    sample_counts = {}

    try:
        for site in cancer_site:
            # ── Resolve site_id ─────────────────────────────────────
            cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
            site_row = cur.fetchone()
            if not site_row:
                continue
            site_id = site_row["id"]

            # ── Resolve cancer types ───────────────────────────────
            if cancer_type:
                cur.execute(
                    "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
                    (site_id, tuple(cancer_type)),
                )
            else:
                cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
            cancer_types = [row["tcga_code"] for row in cur.fetchall()]
            if not cancer_types:
                continue

            # ── Sample counts ──────────────────────────────────────
            cur.execute(
                """
                SELECT s.sample_type, COUNT(*) AS count
                FROM samples s
                JOIN cancer_types c ON c.id = s.cancer_type_id
                WHERE c.site_id = %s AND c.tcga_code IN %s
                GROUP BY s.sample_type
                """,
                (site_id, tuple(cancer_types)),
            )
            counts = {"tumor": 0, "normal": 0}
            for row in cur.fetchall():
                if row["sample_type"].lower() in counts:
                    counts[row["sample_type"].lower()] = row["count"]
            sample_counts[site] = counts

            # ── Initialize site entry ──────────────────────────────
            results[site] = {}

            # ── Process each requested gene ───────────────────────
            for gene_input in gene_ids:
                cur.execute(
                    """
                    SELECT id, ensembl_id, gene_symbol 
                    FROM genes 
                    WHERE ensembl_id = %s OR gene_symbol = %s
                    """,
                    (gene_input, gene_input),
                )
                gene_row = cur.fetchone()
                if not gene_row:
                    continue

                gene_id = gene_row["id"]
                gene_symbol = gene_row["gene_symbol"]
                ensembl_id_full = gene_row["ensembl_id"]          # ← KEEP .17 !

                # Human-readable + unique key
                display_key = f"{gene_symbol} ({ensembl_id_full})"

                results[site][display_key] = {
                    "gene_symbol": gene_symbol,
                    "ensembl_id": ensembl_id_full,                # full version preserved
                }

                # ── Loop over normalization methods ─────────────────
                for norm in ["tpm", "fpkm", "fpkm_uq"]:
                    tumor_vals, normal_vals = [], []

                    for ct in cancer_types:
                        cur.execute(f"""
                            SELECT s.sample_type, e.{norm}
                            FROM gene_expressions e
                            JOIN samples s ON s.id = e.sample_id
                            JOIN cancer_types c ON c.id = s.cancer_type_id
                            WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
                              AND e.{norm} IS NOT NULL
                        """, (site_id, ct, gene_id))
                        df = pd.DataFrame(cur.fetchall())
                        tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"][norm].tolist())
                        normal_vals.extend(df[df["sample_type"].str.lower() == "normal"][norm].tolist())

                    tumor = pd.Series(tumor_vals)
                    normal = pd.Series(normal_vals)

                    # Raw metrics
                    tumor_stats = compute_metrics(tumor)
                    normal_stats = compute_metrics(normal)

                    # Log2(+1) metrics
                    tumor_log = np.log2(tumor + 1)
                    normal_log = np.log2(normal + 1)
                    tumor_log_stats = compute_metrics(tumor_log)
                    normal_log_stats = compute_metrics(normal_log)

                    # Differential noise (log2 fold-change of CV)
                    logfc_raw = np.nan
                    if tumor_stats and normal_stats and normal_stats.get("cv", 0) > 0:
                        logfc_raw = np.log2((tumor_stats["cv"]/100) / (normal_stats["cv"]/100))

                    logfc_log2 = np.nan
                    if tumor_log_stats and normal_log_stats:
                        logfc_log2 = (tumor_log_stats["cv"]/100) - (normal_log_stats["cv"]/100)

                    results[site][display_key][norm] = {
                        "raw": {
                            **(tumor_stats or {}),
                            **{f"{k}_normal": v for k, v in (normal_stats or {}).items()},
                            "log2_fc_cv": float(logfc_raw) if not np.isnan(logfc_raw) else None,
                            "n_tumor": len(tumor),
                            "n_normal": len(normal),
                        },
                        "log2": {
                            **(tumor_log_stats or {}),
                            **{f"{k}_normal": v for k, v in (normal_log_stats or {}).items()},
                            "log2_fc_cv": float(logfc_log2) if not np.isnan(logfc_log2) else None,
                            "n_tumor": len(tumor),
                            "n_normal": len(normal),
                        },
                    }

        final_output = {
            "results": sanitize_floats(results),
            "sample_counts": sample_counts,
        }

    except Exception as e:
        import traceback
        final_output = {"error": str(e), "traceback": traceback.format_exc()}
    finally:
        conn.close()
    return JSONResponse(final_output)

# pathway analysis
class GeneNoiseRequest(BaseModel):
    cancer: List[str]                 # or str if you want comma-separated
    genes: List[str]
    cancer_types: Optional[List[str]] = None

@app.post("/api/gene-noise-pathway")
def get_gene_noise_pathway(req: GeneNoiseRequest = Body(...)):
    # --- normalize cancer sites ---
    cancer_sites = []
    for c in req.cancer:
        if not c:
            continue
        if "," in c or ";" in c:
            cancer_sites.extend([x.strip() for x in re.split(r"[,;]+", c) if x.strip()])
        else:
            cancer_sites.append(c.strip())
    if not cancer_sites:
        raise HTTPException(status_code=400, detail="At least one cancer site is required")

    # --- normalize genes ---
    gene_symbols = []
    for g in req.genes:
        if not g:
            continue
        if "," in g or ";" in g:
            gene_symbols.extend([x.strip().upper() for x in re.split(r"[,;]+", g) if x.strip()])
        else:
            gene_symbols.append(g.strip().upper())
    if not gene_symbols:
        raise HTTPException(status_code=400, detail="At least one gene symbol is required")

    # --- normalize optional cancer types ---
    cancer_type_items: Optional[List[str]] = None
    if req.cancer_types:
        tmp: list[str] = []
        for ct in req.cancer_types:
            if not ct:
                continue
            if "," in ct or ";" in ct:
                tmp.extend([x.strip().upper() for x in re.split(r"[,;]+", ct) if x.strip()])
            else:
                tmp.append(ct.strip().upper())
        cancer_type_items = tmp if tmp else None


    with get_connection() as conn:
        cur = conn.cursor(pymysql.cursors.DictCursor)

        # ---- gene lookup -------------------------------------------------
        cur.execute(
            "SELECT id, ensembl_id, gene_symbol FROM genes WHERE ensembl_id in %s or gene_symbol IN %s",
            (tuple(gene_symbols),)
        )
        gene_rows = cur.fetchall()
        gene_map = {r["ensembl_id"]: r["id"] for r in gene_rows}
        missing = set(gene_symbols) - set(gene_map.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"Genes not found: {', '.join(missing)}")

        sample_counts = get_sample_counts(cur, cancer_sites)

        results = {
            "sample_counts": sample_counts,
            "available_sites": get_available_sites(cur),
            "gene_noise": {norm: {} for norm in ["tpm", "fpkm", "fpkm_uq"]},
        }

        for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
            for site in cancer_sites:
                site_key = site.lower()
                cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
                site_row = cur.fetchone()
                if not site_row:
                    continue
                site_id = site_row["id"]
                # determine which cancer_type (TCGA codes) to use for this site
                if cancer_type_items:
                    cur.execute(
                        "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
                        (site_id, tuple(cancer_type_items)),
                    )
                else:
                    cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
                ct_rows = [r["tcga_code"] for r in cur.fetchall()]
                if not ct_rows:
                    # no matching cancer types for this site → skip site
                    continue

                for gene_symbol, gene_id in gene_map.items():
                    cur.execute(
                        f"""
                        SELECT s.sample_type, e.{norm_method} AS expr
                        FROM gene_expressions e
                        JOIN samples s ON e.sample_id = s.id
                        JOIN cancer_types c ON s.cancer_type_id = c.id
                        WHERE c.site_id = %s AND e.gene_id = %s
                        """,
                        (site_id, gene_id),
                    )
                  # filter by site and (optionally) by cancer_type codes for that site
                    if cancer_type_items:
                        cur.execute(
                            f"""
                            SELECT s.sample_type, e.{norm_method} AS expr
                            FROM gene_expressions e
                            JOIN samples s ON e.sample_id = s.id
                            JOIN cancer_types c ON s.cancer_type_id = c.id
                            WHERE c.site_id = %s AND e.gene_id = %s AND c.tcga_code IN %s
                            """,
                            (site_id, gene_id, tuple(ct_rows)),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT s.sample_type, e.{norm_method} AS expr
                            FROM gene_expressions e
                            JOIN samples s ON e.sample_id = s.id
                            JOIN cancer_types c ON s.cancer_type_id = c.id
                            WHERE c.site_id = %s AND e.gene_id = %s
                            """,
                            (site_id, gene_id),
                        )
                    df = pd.DataFrame(cur.fetchall())
                    if df.empty:
                        continue

                    # ---- split tumor / normal ---------------------------------
                    tumor = df[df["sample_type"].str.lower() == "tumor"]["expr"]
                    normal = df[df["sample_type"].str.lower() == "normal"]["expr"]

                    # ---- compute tumor stats ----------------------------------
                    t_mean = float(tumor.mean()) if not tumor.empty else None
                    t_cv   = (float(tumor.std(ddof=1)) / t_mean * 100) if t_mean is not None else None

                    # ---- compute normal stats (may be empty) ------------------
                    n_mean = float(normal.mean()) if not normal.empty else None
                    n_cv   = (float(normal.std(ddof=1)) / n_mean * 100) if n_mean is not None else None

                    # ---- log2 for logfc ---------------------------------------
                    df["log2_expr"] = np.log2(df["expr"] + 1)
                    t_log = df[df["sample_type"].str.lower() == "tumor"]["log2_expr"]
                    n_log = df[df["sample_type"].str.lower() == "normal"]["log2_expr"]

                    t_cv_log = (float(t_log.std(ddof=1)) / t_log.mean() * 100) if t_log.mean() else None
                    n_cv_log = (float(n_log.std(ddof=1)) / n_log.mean() * 100) if n_log.mean() else None

                    logfc = None
                    if t_cv_log is not None and n_cv_log is not None:
                        logfc = float((t_cv_log/100) - (n_cv_log/100))

                    # ---- store only 5 fields ----------------------------------
                    results["gene_noise"][norm_method].setdefault(gene_symbol, {})[site_key] = {
                        "mean_tumor":   t_mean,
                        "mean_normal":  n_mean,
                        "cv_tumor":     t_cv,
                        "cv_normal":    n_cv,
                        "logfc":        logfc,
                    }

        return JSONResponse(sanitize_floats(results))
    
# pathway analysis 
# enriched pathways

class Pathway(BaseModel):
    id: str
    value: str
    label: str
    category: str
    genes: List[str]


@app.post("/api/enriched-pathways", response_model=List[Pathway])
async def get_enriched_pathways(request: GeneRequest):
    """
    Query STRING-DB enrichment API for the provided genes.
    Returns only KEGG and GO pathways, with IDs included.
    """
    try:
        # Make STRING API request
        response = requests.post(
            "https://string-db.org/api/json/enrichment",
            data={
                "identifiers": "\n".join(request.genes),
                "species": 9606,
                "caller_identity": "app",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        pathways = []
        for item in data:
            category = item.get("category", "").lower()

            # Filter: keep only KEGG and GO terms
            if not any(cat in category for cat in ["kegg", "process", "function", "component"]):
                continue

            # Skip publication/PMID categories
            if any(kw in category for kw in ["pubmed", "pmid", "publication"]):
                continue

            # Extract gene list
            genes_raw = item.get("inputGenesList") or item.get("inputGenes") or []
            if isinstance(genes_raw, str):
                genes = [g.strip() for g in genes_raw.split(",") if g.strip()]
            elif isinstance(genes_raw, list):
                genes = genes_raw
            else:
                genes = []

            # Build the structured response
            pathways.append({
                "id": item.get("term", ""),
                "value": item.get("term", ""),
                "description": item.get("description", ""),
                "label": f"{item.get('description', 'Unknown')} "
                         f"(FDR: {item.get('fdr')}, Category: {item.get('category')})",
                "category": item.get("category", ""),
                "genes": genes,
            })

        return pathways

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch enriched pathways: {str(e)}")

@app.get("/api/get-genes")
def get_genes_for_pathway(pathway: str):
    """Return all gene symbols for a given KEGG or GO pathway."""
    if not pathway:
        raise HTTPException(status_code=400, detail="Pathway ID required")

    if pathway.upper().startswith("GO:"):
        genes = fetch_go_genes(pathway)
    else:
        genes = fetch_kegg_genes(pathway)

    return {"pathway": pathway, "genes": genes}

class PathwayAnalysisRequest(BaseModel):
    cancer: str
    genes: Optional[List[str]] = None
    top_n: int = 1000
    logfc_threshold: float = 0.7
    mode: str = "enrichment"
    network_type: str = "functional"
    score_threshold: float = 0.4
    pathway: Optional[str] = None


@app.post("/api/pathway-analysis")
def post_pathway_analysis(req: PathwayAnalysisRequest):
    """
    Perform pathway or network analysis for genes across cancer sites.
    If `pathway` (KEGG or GO ID) is provided, fetch its genes from KEGG or QuickGO first.
    """
    cancer_sites = [s.strip() for s in req.cancer.split(",") if s.strip()]
    

    with get_connection() as conn:
        cur = conn.cursor()
        gene_list, selected_gene_set, warning = get_genes(
            cur,
            req.genes,
            cancer_sites,
            req.logfc_threshold
        )

        # --- Existing downstream analysis ---
        sample_counts = get_sample_counts(cur, cancer_sites)
        results = compute_gene_stats(cur, cancer_sites, gene_list)

        string_results = (
            run_stringdb_enrichment(selected_gene_set, req.top_n)
        )

        # attach results
        for scale in ["raw", "log2"]:
            for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
                results[scale][norm_method]["top_genes"] = list(selected_gene_set)
                results[scale][norm_method][req.mode] = string_results

        results = sanitize_floats({
            "raw": results["raw"],
            "log2": results["log2"],
            "sample_counts": sample_counts,
            "available_sites": get_available_sites(cur),
            "warning": warning,
            "mode": req.mode,
        })

        return JSONResponse(sanitize_floats(results))

def run_stringdb_enrichment(gene_set: Set[str], top_n: int, species: int = 9606) -> List[Dict]:
    """Run STRINGdb enrichment for a gene set."""
    
    if not gene_set:
        return []
    try:
        response = requests.get(f"https://string-db.org/api/json/enrichment?identifiers={'%0D'.join(gene_set)}&species={species}")
        response.raise_for_status()
        data = response.json()
        category_map = {"Process": "GO:BP", "Function": "GO:MF", "Component": "GO:CC", "KEGG": "KEGG"}
        valid_categories = set(category_map.values())
        results = [
            {
                "Term": item["term"],
                "Database": category_map.get(item["category"], "Unknown"),
                "FDR": item["fdr"],
                "MatchingGenes": item["inputGenes"],
                "Description": item["description"],
                "GeneCount": len(item["inputGenes"]),
                "EnrichmentScore": -math.log10(item["fdr"] + 1e-10),
                # "EnrichmentScore": item["p-value"],
                "GeneSet": "STRINGdb"
            }
            for item in sorted(data, key=lambda x: x["fdr"])[:top_n]
            if category_map.get(item["category"]) in valid_categories
        ]
        return results
    except Exception as e:
        logger.error(f"STRING enrichment failed: {e}")
        return []

def get_genes(cur, genes_input: List[str], cancer_sites: List[str], logfc_threshold: float) -> tuple[List[int], Set[str], Optional[str]]:
        """Get gene list from input or auto-select top DE genes."""
        if genes_input:
            unique_genes = list(dict.fromkeys(g.strip().upper() for gene in genes_input for g in str(gene).split(",") if g.strip()))
            gene_list, gene_set = [], set()
            for gene_symbol in unique_genes:
                cur.execute("SELECT id, gene_symbol FROM genes WHERE gene_symbol = %s OR ensembl_id = %s", (gene_symbol, gene_symbol))
                row = cur.fetchone()
                if row:
                    gene_list.append(row["id"])
                    gene_set.add(row["gene_symbol"])
            return gene_list, gene_set, f"Found {len(gene_list)}/{len(unique_genes)} genes" if len(gene_list) < len(unique_genes) else None

    
def get_sample_counts(cur, cancer_sites: List[str]) -> Dict[str, Dict[str, int]]:
    """Get tumor/normal sample counts per site."""
    counts = {}
    for site in cancer_sites:
        cur.execute(
            """
            SELECT s.sample_type, COUNT(*) as count
            FROM samples s JOIN cancer_types c ON s.cancer_type_id = c.id JOIN sites si ON c.site_id = si.id
            WHERE si.name = %s GROUP BY s.sample_type
            """, (site,))
        site_counts = {"tumor": 0, "normal": 0}
        for row in cur.fetchall():
            site_counts[row["sample_type"].lower()] = row["count"]
        counts[site] = site_counts
    return counts

def compute_gene_stats(cur, cancer_sites: List[str], gene_list: List[int]) -> Dict[str, Dict[str, Dict]]:
    """Compute stats for genes across normalization methods and scales."""
    results = {scale: {norm: {"gene_stats": {}, "heatmap_data": {}, "noise_metrics": {}, "enrichment": [], "network": [], "top_genes": []}
                      for norm in ["tpm", "fpkm", "fpkm_uq"]} for scale in ["raw", "log2"]}
    
    for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
        for gene_id in gene_list:
            cur.execute("SELECT ensembl_id, gene_symbol FROM genes WHERE id = %s", (gene_id,))
            gene_row = cur.fetchone()
            gene_symbol = gene_row["gene_symbol"]
            ensembl_id = gene_row["ensembl_id"]
            for site in cancer_sites:
                cur.execute(
                    f"""
                    SELECT s.sample_type, e.{norm_method} as expr
                    FROM gene_expressions e JOIN samples s ON e.sample_id = s.id
                    JOIN cancer_types c ON s.cancer_type_id = c.id JOIN sites si ON c.site_id = si.id
                    WHERE si.name = %s AND e.gene_id = %s
                    """, (site, gene_id))
                df = pd.DataFrame(cur.fetchall())
                if df.empty:
                    continue
                
                tumor = df[df["sample_type"].str.lower() == "tumor"]["expr"]
                normal = df[df["sample_type"].str.lower() == "normal"]["expr"]
                tumor_stats, normal_stats = compute_metrics(tumor), compute_metrics(normal)
                
                df["log2_expr"] = np.log2(df["expr"] + 1)
                tumor_log = df[df["sample_type"].str.lower() == "tumor"]["log2_expr"]
                normal_log = df[df["sample_type"].str.lower() == "normal"]["log2_expr"]
                tumor_stats_log, normal_stats_log = compute_metrics(tumor_log), compute_metrics(normal_log)
                
                site_key = site.lower()
                for scale, t_stats, n_stats in [("raw", tumor_stats, normal_stats), ("log2", tumor_stats_log, normal_stats_log)]:
                    # safely handle missing tumor/normal stats
                    t = t_stats or {}
                    n = n_stats or {}
                    # compute logfc using available cv values, otherwise NaN
                    logfc_val = (((t.get("cv")/100) - (n.get("cv")/100))
                                 if (t.get("cv") is not None and n.get("cv") is not None)
                                 else float("nan"))

                    # results[scale][norm_method]["gene_stats"].setdefault(gene_symbol, {})[site_key] = {
                    results[scale][norm_method]["gene_stats"].setdefault(gene_symbol, {})[site_key] = {
                            "ensembl_id": ensembl_id,   # ← ADD THIS LINE
                            **{f"{k}_tumor": v for k, v in t.items()},
                            **{f"{k}_normal": v for k, v in n.items()},
                            "logfc": logfc_val,
                        }
                        # **{f"{k}_tumor": v for k, v in t.items()},
                        # **{f"{k}_normal": v for k, v in n.items()},
                        # "logfc": logfc_val,
                    # }
                    # ensure heatmap_data entry exists and write mean safely
                    # results[scale][norm_method]["heatmap_data"].setdefault(gene_symbol, {})["ensembl_id"] = ensembl_id
                    # results[scale][norm_method]["noise_metrics"].setdefault(gene_symbol, {})[site_key]["ensembl_id"] = ensembl_id
                    results[scale][norm_method]["heatmap_data"].setdefault(gene_symbol, {})
                    results[scale][norm_method]["heatmap_data"][gene_symbol][f"{site}_normal"] = n.get("mean", 0)
                    results[scale][norm_method]["heatmap_data"][gene_symbol][f"{site}_tumor"] = t.get("mean", 0)
                    results[scale][norm_method]["noise_metrics"].setdefault(gene_symbol, {})[site_key] = {
                        "cv_tumor": t.get("cv", 0),
                        "cv_normal": n.get("cv", 0),
                        "logfc": logfc_val,
                   }
    return sanitize_floats(results)

# cancer project selector
@app.get("/api/cancer_types")
def get_cancer_types(site_ids: List[int] = Query(...)):
    """Fetch cancer types for given site IDs."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT tcga_code, site_id FROM cancer_types WHERE site_id IN %s", (tuple(site_ids),))
        return {"cancer_types": cur.fetchall()}


def get_available_sites(cur) -> List[Dict]:
    """Fetch all available sites."""
    cur.execute("SELECT id, name FROM sites ORDER BY name")
    return cur.fetchall()

# tumor analysis
@app.get("/api/tumor_results")
def get_tumor_results(cancer_site: str = Query(...), cancer_types: Optional[List[str]] = Query(None)):
    """Fetch tumor sample data with DEPTH2 and DEPTH scores."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM sites WHERE name = %s", (cancer_site,))
        site_row = cur.fetchone()
        if not site_row:
            raise HTTPException(status_code=404, detail=f"Cancer site '{cancer_site}' not found.")
        site_id = site_row["id"]

        query = "SELECT id, tcga_code FROM cancer_types WHERE site_id = %s"
        params = [site_id]
        if cancer_types:
            query += " AND tcga_code IN %s"
            params.append(tuple(cancer_types))
        cur.execute(query, params)
        cancer_type_rows = cur.fetchall()
        if not cancer_type_rows:
            raise HTTPException(status_code=404, detail=f"No cancer types found for site '{cancer_site}'.")
        
        cancer_type_ids = [row["id"] for row in cancer_type_rows]
        cancer_type_map = {row["id"]: row["tcga_code"] for row in cancer_type_rows}

        cur.execute(
            """
            SELECT s.sample_type, COUNT(*) AS count
            FROM samples s JOIN cancer_types c ON c.id = s.cancer_type_id
            WHERE c.site_id = %s AND c.id IN %s GROUP BY s.sample_type
            """, (site_id, tuple(cancer_type_ids)))
        sample_counts = {"tumor": 0, "normal": 0}
        for row in cur.fetchall():
            sample_counts[row["sample_type"].lower()] = row["count"]

        cur.execute(
            """
            SELECT s.id AS sample_id, s.sample_barcode, s.cancer_type_id,
                   d.tpm AS tpm_depth2, d.fpkm AS fpkm_depth2, d.fpkm_uq AS fpkm_uq_depth2,
                   t.tpm AS tpm_depth, t.fpkm AS fpkm_depth, t.fpkm_uq AS fpkm_uq_depth
            FROM samples s JOIN cancer_types c ON c.id = s.cancer_type_id
            LEFT JOIN depth2_scores d ON d.sample_id = s.id
            LEFT JOIN tith_scores t ON t.sample_id = s.id
            WHERE c.site_id = %s AND c.id IN %s AND s.sample_type = 'tumor'
            """, (site_id, tuple(cancer_type_ids)))
        
        results = [
            {
                "sample_id": row["sample_id"],
                "sample_barcode": row["sample_barcode"],
                "cancer_type": cancer_type_map.get(row["cancer_type_id"], "Unknown"),
                **{k: row[k] for k in ["tpm_depth2", "fpkm_depth2", "fpkm_uq_depth2", "tpm_depth", "fpkm_depth", "fpkm_uq_depth"]}
            } for row in cur.fetchall()
        ]
        return JSONResponse(sanitize_floats({
            "results": results,
            "sample_counts": sample_counts,
            "error": None
        }))

@app.post("/api/csv-upload")
async def csv_upload(request: Request):
    try:
        form = await request.form()
        analysis_type = form.get('analysis_type', 'Pathway')
        top_n = int(form.get('top_n', 15))
        gene_set = form.get('gene_set', 'KEGG_2021_Human') if analysis_type == 'Pathway' else None
        genes = form.get('genes', '')
        print(genes)

        logger.info(f"Input: analysis_type={analysis_type}, top_n={top_n}, gene_set={gene_set}, genes={genes}")

        valid_analysis_types = ['Gene', 'Pathway', 'Tumor']
        if analysis_type not in valid_analysis_types:
            return JSONResponse({"error": f"Invalid analysis type: {analysis_type}"}, status_code=400)

        selected_genes = [g.strip().upper() for g in genes.split(',') if g.strip()] if genes.strip() else None
        print(selected_genes)
        if analysis_type in ['Gene', 'Pathway'] and not selected_genes and 'expression_file' not in form:
            return JSONResponse({"error": "Gene or Pathway Analysis requires a gene list or expression data"}, status_code=400)

        # Initialize response
        response = {
            "analysis_type": analysis_type,
            "raw": {"warning": "Differential noise analysis (e.g., logFC) is not possible"},
            "log2": {"warning": "Differential noise analysis (e.g., logFC) is not possible"}
        } if analysis_type != 'Tumor' else {
            "analysis_type": analysis_type,
            "log2": {"warning": ""}
        }

        # Process uploaded expression file
        df_raw = None
        df_log2 = None
        if 'expression_file' in form:
            expression_file = form['expression_file']
            if not expression_file.filename:
                return JSONResponse({"error": "No expression file selected"}, status_code=400)
            if not expression_file.filename.endswith(('.csv', '.tsv')):
                return JSONResponse({"error": "Expression file must be CSV or TSV"}, status_code=400)
            delimiter = ',' if expression_file.filename.endswith('.csv') else '\t'

            # Read the CSV/TSV file
            content = await expression_file.read()
            df_raw = pd.read_csv(io.BytesIO(content), delimiter=delimiter)
            if not {'Hugo_Symbol', 'gene_name'}.intersection(df_raw.columns):
                return JSONResponse({"error": "Expression file must contain 'Hugo_Symbol' or 'gene_name' column"}, status_code=400)

            gene_column = 'gene_name' if 'gene_name' in df_raw.columns else 'Hugo_Symbol'
            gene_id = 'gene_id' if 'gene_id' in df_raw.columns else 'Entrez_Gene_Id' if 'Entrez_Gene_Id' in df_raw.columns else None
            df_raw[gene_column] = df_raw[gene_column].astype(str).str.upper()

            # Prepare raw and log2 data
            df_raw = df_raw.set_index(gene_column)
            if gene_id in df_raw.columns:
                df_raw = df_raw.drop(columns=[gene_id])
            df_raw = df_raw.apply(pd.to_numeric, errors='coerce')
            df_raw = df_raw.fillna(df_raw.median(numeric_only=True))
            df_log2 = np.log2(df_raw + 1)

        # Handle Gene Analysis
        if analysis_type == 'Gene':
            for transform, df in [('raw', df_raw), ('log2', df_log2)]:
                if df is not None:
                    top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
                    if not top_genes:
                        return JSONResponse({"error": f"None of the provided genes found in {transform} expression data"}, status_code=400)
                    df = df.loc[top_genes]
                    metrics = {name: func(df).to_dict() for name, func in metric_funcs_gene.items()}
                else:
                    if not selected_genes:
                        return JSONResponse({"error": "Gene Analysis requires a gene list or expression data"}, status_code=400)
                    top_genes = selected_genes
                    metrics = {name: {gene: 0.0 for gene in top_genes} for name in metric_funcs_gene.keys()}
                response[transform].update({"metrics": metrics, "top_genes": top_genes})

        # Handle Pathway Analysis
        elif analysis_type == 'Pathway':
            for transform, df in [('raw', df_raw), ('log2', df_log2)]:
                if df is not None:
                    top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
                    if not top_genes:
                        return JSONResponse({"error": f"None of the provided genes found in {transform} expression data"}, status_code=400)
                    df = df.loc[top_genes]
                    cv_values = metric_funcs_gene['cv'](df)
                    heatmap_data = {gene: {"mean": float(df.loc[gene].mean()), "cv": float(cv_values.get(gene, 0.0))} for gene in top_genes}
                else:
                    if not selected_genes:
                        return JSONResponse({"error": "Pathway Analysis requires a gene list or expression data"}, status_code=400)
                    top_genes = selected_genes
                    heatmap_data = {gene: {"mean": 0.0, "cv": 0.0} for gene in top_genes}

                enrichment_results = []
                try:
                    # Run STRING-db enrichment
                    string_results = run_stringdb_enrichment(gene_set=set(top_genes), top_n=int(top_n))

                    # Convert to your exact response format
                    for res in string_results:
                        fdr = res["FDR"]
                        p_val = fdr  # Use FDR as p-value (common in enrichment)
                        combined_score = res["EnrichmentScore"]  # -log10(FDR)
                        gene_list = [g["preferred_name"] if isinstance(g, dict) else str(g) for g in res["MatchingGenes"]]

                        enrichment_results.append({
                            "Term": res["Term"],
                            "FDR": float(fdr),
                            "Genes": gene_list,
                            "GeneSet": f"STRINGdb_{res['Database']}"  # e.g., STRINGdb_KEGG
                        })

                except Exception as e:
                    logger.error(f"Failed to process STRING enrichment for {transform}: {e}")

                response[transform].update({
                    "enrichment": enrichment_results,
                    "top_genes": top_genes,
                    "heatmap_data": heatmap_data
                })

        # Handle Tumor Analysis
        elif analysis_type == 'Tumor':
            if df_log2 is None:
                return JSONResponse({"error": "Tumor Analysis requires an expression data file"}, status_code=400)
            all_metrics = {}
            for metric_name, func in metric_funcs_TH.items():
                try:
                    metric_series = func(df_log2)  # Compute across all samples
                    all_metrics[metric_name] = metric_series
                except Exception as e:
                    logger.error(f"Failed to compute {metric_name}: {e}")
                    all_metrics[metric_name] = pd.Series(0.0, index=df_log2.columns)

            metrics = []
            for sample in df_log2.columns:
                sample_metrics = {"sample": sample}
                for metric_name, metric_series in all_metrics.items():
                    val = metric_series.get(sample, 0.0)
                    sample_metrics[metric_name] = float(val) if pd.notna(val) else 0.0
                metrics.append(sample_metrics)
            response["log2"]["metrics"] = metrics

        logger.info(f"CSV upload response generated for {analysis_type} analysis")
        return JSONResponse(response)

    except Exception as e:
        logger.error(f"CSV upload failed: {e}")
        return JSONResponse({"error": f"Internal server error: {str(e)}"}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5001)