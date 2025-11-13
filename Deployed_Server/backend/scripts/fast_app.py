from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional, Set
from cv import cv_calculation
from std import std_calculation
from MAD import mad_calculation
from DEPTH2 import depth2_calculation
from DEPTH_ITH import depth_calculation
from cv_2 import cv2_calculation
from mean import mean_calculation
import pymysql
import pandas as pd
import numpy as np
import math
import scipy.stats as stats
from statsmodels.stats.multitest import multipletests
import requests
import logging
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

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "rubab",
    "password": "initiate123",
    "database": "cancer_db",
    "cursorclass": pymysql.cursors.DictCursor,
}

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
    "tITH": depth_calculation,
}

def get_connection():
    """Create and return a database connection."""
    return pymysql.connect(**DB_CONFIG)

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

class Pathway(BaseModel):
    value: str
    label: str
    category: str

# def compute_metrics(values: pd.Series) -> Dict[str, float]:
#     """Compute statistical metrics for a numeric series."""
#     if values.empty:
#         return {}
#     metrics = {name: func(values) for name, func in metric_funcs_gene.items()}
#     return {k: v for k, v in metrics.items() if not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))}
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


@app.get("/api/genes")
def get_genes():
    """Fetch all available gene symbols."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT gene_symbol FROM genes WHERE gene_symbol IS NOT NULL ORDER BY gene_symbol")
        return {"genes": [row["gene_symbol"] for row in cur.fetchall()]}

@app.get("/api/sites")
def get_sites():
    """Fetch all available cancer sites."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM sites ORDER BY name")
        return {"sites": cur.fetchall()}

@app.get("/api/cancer_types")
def get_cancer_types(site_ids: List[int] = Query(...)):
    """Fetch cancer types for given site IDs."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT tcga_code, site_id FROM cancer_types WHERE site_id IN %s", (tuple(site_ids),))
        return {"cancer_types": cur.fetchall()}

@app.get("/api/gene_noise")
def get_gene_noise(
    cancer_site: List[str] = Query(...),
    cancer_type: Optional[List[str]] = Query(None),
    gene_ids: List[str] = Query(...)
):
    """Compute expression noise metrics for cancer sites and genes."""
    results = {}
    sample_counts = {}
    
    with get_connection() as conn:
        cur = conn.cursor()
        for site in cancer_site:
            cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
            site_row = cur.fetchone()
            if not site_row:
                continue
            site_id = site_row["id"]

            # Fetch cancer types
            query = "SELECT tcga_code FROM cancer_types WHERE site_id = %s"
            params = [site_id]
            if cancer_type:
                query += " AND tcga_code IN %s"
                params.append(tuple(cancer_type))
            cur.execute(query, params)
            cancer_types = [row["tcga_code"] for row in cur.fetchall()]
            if not cancer_types:
                continue

            # Sample counts
            cur.execute(
                """
                SELECT s.sample_type, COUNT(*) AS count
                FROM samples s JOIN cancer_types c ON c.id = s.cancer_type_id
                WHERE c.site_id = %s AND c.tcga_code IN %s
                GROUP BY s.sample_type
                """,
                (site_id, tuple(cancer_types))
            )
            sample_counts[site] = {"tumor": 0, "normal": 0}
            for row in cur.fetchall():
                sample_counts[site][row["sample_type"].lower()] = row["count"]

            results[site] = {}
            for gene_input in gene_ids:
                cur.execute(
                    "SELECT id, gene_symbol FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
                    (gene_input, gene_input)
                )
                gene_row = cur.fetchone()
                if not gene_row:
                    continue
                gene_id, gene_symbol = gene_row["id"], gene_row["gene_symbol"]
                results[site][gene_symbol] = {}

                for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
                    tumor_vals, normal_vals = [], []
                    for ct in cancer_types:
                        cur.execute(
                            f"""
                            SELECT s.sample_type, e.{norm_method} AS expr
                            FROM gene_expressions e
                            JOIN samples s ON s.id = e.sample_id
                            JOIN cancer_types c ON c.id = s.cancer_type_id
                            WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
                            """,
                            (site_id, ct, gene_id)
                        )
                        df = pd.DataFrame(cur.fetchall())
                        tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"])
                        normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"])

                    tumor, normal = pd.Series(tumor_vals), pd.Series(normal_vals)
                    tumor_stats, normal_stats = compute_metrics(tumor), compute_metrics(normal)
                    
                    # Log2-transformed data
                    df_all = pd.DataFrame({"expr": list(tumor) + list(normal)})
                    df_all["log2_expr"] = np.log2(df_all["expr"] + 1)
                    tumor_log = df_all["log2_expr"][:len(tumor)]
                    normal_log = df_all["log2_expr"][len(tumor):]
                    tumor_stats_log, normal_stats_log = compute_metrics(tumor_log), compute_metrics(normal_log)
                    
                    logfc = tumor_stats_log.get("cv", 0) - normal_stats_log.get("cv", 0) if tumor_stats_log and normal_stats_log else np.nan
                    results[site][gene_symbol][norm_method] = {
                        "raw": {**tumor_stats, **{f"{k}_normal": v for k, v in normal_stats.items()}, "logfc": logfc},
                        "log2": {**tumor_stats_log, **{f"{k}_normal": v for k, v in normal_stats_log.items()}, "logfc": logfc},
                    }

    return JSONResponse(sanitize_floats({"results": results, "sample_counts": sample_counts}))

@app.get("/api/pathway-analysis")
def get_pathway_analysis(
    cancer: str = Query(...),
    genes: Optional[List[str]] = Query(None),
    top_n: int = Query(50),
    logfc_threshold: float = Query(0.7),
    mode: str = Query("enrichment"),
    network_type: str = Query("functional"),
    score_threshold: float = Query(0.4),
    pathway: Optional[str] = Query(None)
):
    """Perform pathway or network analysis for genes across cancer sites."""
    cancer_sites = [s.strip() for s in cancer.split(",") if s.strip()]
    with get_connection() as conn:
        cur = conn.cursor()
        gene_list, selected_gene_set, warning = get_genes(cur, genes, cancer_sites, logfc_threshold)
        
        if pathway:
            response = requests.get(f"https://string-db.org/api/json/enrichment?identifiers={'%0D'.join(genes)}&species=9606")
            response.raise_for_status()
            data = response.json()
            pathway_data = next((item for item in data if item["term"] == pathway), None)
            if not pathway_data:
                raise HTTPException(status_code=404, detail=f"Pathway '{pathway}' not found.")
            selected_gene_set = set(pathway_data["inputGenes"])
            cur.execute("SELECT id, gene_symbol FROM genes WHERE gene_symbol IN %s", (tuple(selected_gene_set),))
            gene_list = [row["id"] for row in cur.fetchall()]
            selected_gene_set = set(row["gene_symbol"] for row in cur.fetchall())

        sample_counts = get_sample_counts(cur, cancer_sites)
        results = compute_gene_stats(cur, cancer_sites, gene_list)
        string_results = run_stringdb_neighbors(selected_gene_set, network_type, score_threshold, top_n) if mode == "neighbors" else run_stringdb_enrichment(selected_gene_set, top_n)

        for scale in ["raw", "log2"]:
            for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
                results[scale][norm_method]["top_genes"] = list(selected_gene_set)
                results[scale][norm_method][mode] = string_results

        results = sanitize_floats({
            "raw": results["raw"],
            "log2": results["log2"],
            "sample_counts": sample_counts,
            "available_sites": get_available_sites(cur),
            "warning": warning,
            "mode": mode,
        })
        print(results)
        return JSONResponse(results)

def run_stringdb_enrichment(gene_set: Set[str], top_n: int, species: int = 9606) -> List[Dict]:
    """Run STRINGdb enrichment for a gene set."""
    if not gene_set:
        return []
    try:
        response = requests.get(f"https://string-db.org/api/json/enrichment?identifiers={'%0D'.join(gene_set)}&species={species}")
        response.raise_for_status()
        data = response.json()
        category_map = {"Process": "GO:BP", "Function": "GO:MF", "Component": "GO:CC", "KEGG": "KEGG", "Reactome": "Reactome", "UniProt": "UniProt", "WikiPathways": "WikiPathways"}
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
                "GeneSet": "STRINGdb"
            }
            for item in sorted(data, key=lambda x: x["fdr"])[:top_n]
            if category_map.get(item["category"]) in valid_categories
        ]
        return results
    except Exception as e:
        logger.error(f"STRING enrichment failed: {e}")
        return []

def run_stringdb_neighbors(gene_set: Set[str], network_type: str, score_threshold: float, top_n: int, species: int = 9606) -> List[Dict]:
    """Get nearest neighbor network from STRINGdb."""
    if not gene_set:
        return []
    try:
        response = requests.get(f"https://string-db.org/api/json/network?identifiers={'%0D'.join(gene_set)}&species={species}")
        response.raise_for_status()
        data = response.json()
        filtered = [x for x in data if x["score"] >= score_threshold and (network_type != "physical" or x.get("network_type") == "physical")]
        return [
            {"source": x["preferredName_A"], "target": x["preferredName_B"], "score": x["score"]}
            for x in sorted(filtered, key=lambda x: x["score"], reverse=True)[:top_n]
        ]
    except Exception as e:
        logger.error(f"STRING neighbors failed: {e}")
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

    gene_list, gene_set = [], set()
    for site in cancer_sites[:2]:
        cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
        site_row = cur.fetchone()
        if not site_row:
            continue
        cur.execute(
            """
            SELECT g.id, g.gene_symbol
            FROM genes g JOIN gene_expressions ge ON g.id = ge.gene_id
            JOIN samples s ON ge.sample_id = s.id JOIN cancer_types c ON s.cancer_type_id = c.id
            WHERE c.site_id = %s
            GROUP BY g.id, g.gene_symbol
            HAVING AVG(CASE WHEN s.sample_type = 'Normal' THEN ge.tpm END) > 0
            AND AVG(CASE WHEN s.sample_type = 'Tumor' THEN ge.tpm END) > 0
            ORDER BY ABS(LOG2(AVG(CASE WHEN s.sample_type = 'Tumor' THEN ge.tpm END)/AVG(CASE WHEN s.sample_type = 'Normal' THEN ge.tpm END))) DESC
            LIMIT 10
            """, (site_row["id"],))
        for row in cur.fetchall():
            gene_list.append(row["id"])
            gene_set.add(row["gene_symbol"])
    return list(set(gene_list)), gene_set, "No genes provided. Using top DE genes."

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
            cur.execute("SELECT gene_symbol FROM genes WHERE id = %s", (gene_id,))
            gene_symbol = cur.fetchone()["gene_symbol"]
            
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
                    results[scale][norm_method]["gene_stats"].setdefault(gene_symbol, {})[site_key] = {
                        **t_stats, **{f"{k}_normal": v for k, v in n_stats.items()},
                        "logfc": math.log2(t_stats["cv"] / n_stats["cv"]) if t_stats and n_stats and n_stats.get("cv", 0) else np.nan
                    }
                    results[scale][norm_method]["heatmap_data"].setdefault(gene_symbol, {})[f"{site}_normal"] = n_stats.get("mean", 0)
                    results[scale][norm_method]["heatmap_data"][gene_symbol][f"{site}_tumor"] = t_stats.get("mean", 0)
                    results[scale][norm_method]["noise_metrics"].setdefault(gene_symbol, {})[site_key] = {
                        "cv_tumor": t_stats.get("cv", 0), "cv_normal": n_stats.get("cv", 0),
                        "logfc": math.log2(t_stats["cv"] / n_stats["cv"]) if t_stats and n_stats and n_stats.get("cv", 0) else np.nan
                    }
    return results

def get_available_sites(cur) -> List[Dict]:
    """Fetch all available sites."""
    cur.execute("SELECT id, name FROM sites ORDER BY name")
    return cur.fetchall()

@app.get("/api/tumor_results")
def get_tumor_results(cancer_site: str = Query(...), cancer_types: Optional[List[str]] = Query(None)):
    """Fetch tumor sample data with DEPTH2 and tITH scores."""
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
                   t.tpm AS tpm_tith, t.fpkm AS fpkm_tith, t.fpkm_uq AS fpkm_uq_tith
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
                **{k: row[k] for k in ["tpm_depth2", "fpkm_depth2", "fpkm_uq_depth2", "tpm_tith", "fpkm_tith", "fpkm_uq_tith"]}
            } for row in cur.fetchall()
        ]

        return JSONResponse(sanitize_floats({"results": results, "sample_counts": sample_counts, "error": None}))

@app.post("/api/csv-upload")
async def csv_upload(request: Request):
    """Handle CSV/TSV file upload for gene, pathway, or tumor analysis."""
    try:
        form = await request.form()
        analysis_type = form.get('analysis_type', 'Pathway')
        top_n = int(form.get('top_n', 15))
        gene_set = form.get('gene_set', 'KEGG_2021_Human') if analysis_type == 'Pathway' else None
        genes = form.get('genes', '')
        
        if analysis_type not in ['Gene', 'Pathway', 'Tumor']:
            raise HTTPException(status_code=400, detail=f"Invalid analysis type: {analysis_type}")

        selected_genes = [g.strip().upper() for g in genes.split(',') if g.strip()] if genes.strip() else None
        if analysis_type in ['Gene', 'Pathway'] and not selected_genes and 'expression_file' not in form:
            raise HTTPException(status_code=400, detail="Gene or Pathway Analysis requires a gene list or expression data")

        response = {
            "analysis_type": analysis_type,
            "raw": {"warning": "Differential noise analysis not possible"},
            "log2": {"warning": "Differential noise analysis not possible" if analysis_type != 'Tumor' else ""}
        }

        df_raw = None
        if 'expression_file' in form:
            expression_file = form['expression_file']
            if not expression_file.filename.endswith(('.csv', '.tsv')):
                raise HTTPException(status_code=400, detail="Expression file must be CSV or TSV")
            df_raw = pd.read_csv(io.BytesIO(await expression_file.read()), delimiter=',' if expression_file.filename.endswith('.csv') else '\t')
            
            gene_column = 'gene_name' if 'gene_name' in df_raw.columns else 'Hugo_Symbol'
            if gene_column not in df_raw.columns:
                raise HTTPException(status_code=400, detail="Expression file must contain 'Hugo_Symbol' or 'gene_name' column")
            
            df_raw[gene_column] = df_raw[gene_column].astype(str).str.upper()
            df_raw = df_raw.set_index(gene_column).select_dtypes(include=np.number).fillna(df_raw.median(numeric_only=True))
            df_log2 = np.log2(df_raw + 1)

        if analysis_type == 'Gene':
            for transform, df in [('raw', df_raw), ('log2', df_log2)]:
                top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
                if df is not None and not top_genes:
                    raise HTTPException(status_code=400, detail=f"None of the provided genes found in {transform} data")
                metrics = {name: func(df.loc[top_genes]).to_dict() for name, func in metric_funcs_gene.items()} if df is not None else {name: {gene: 0.0 for gene in selected_genes} for name in metric_funcs_gene}
                response[transform].update({"metrics": metrics, "top_genes": top_genes})

        elif analysis_type == 'Pathway':
            for transform, df in [('raw', df_raw), ('log2', df_log2)]:
                top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
                if df is not None and not top_genes:
                    raise HTTPException(status_code=400, detail=f"None of the provided genes found in {transform} data")
                heatmap_data = {gene: {"mean": float(df.loc[gene].mean()), "cv": float(metric_funcs_gene['cv'](df.loc[gene]))} for gene in top_genes} if df is not None else {gene: {"mean": 0.0, "cv": 0.0} for gene in selected_genes}
                response[transform].update({"enrichment": [], "top_genes": top_genes, "heatmap_data": heatmap_data})

        elif analysis_type == 'Tumor':
            if df_log2 is None:
                raise HTTPException(status_code=400, detail="Tumor Analysis requires expression data")
            metrics = []
            for sample in df_log2.columns:
                sample_metrics = {"sample": sample}
                for name, func in metric_funcs_TH.items():
                    val = func(df_log2[sample])
                    sample_metrics[name] = float(val) if pd.notna(val) else 0.0
                metrics.append(sample_metrics)
            response["log2"]["metrics"] = metrics

        return JSONResponse(response)
    except Exception as e:
        logger.error(f"CSV upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5001)

# from fastapi import FastAPI, Query, HTTPException, Request
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from typing import List, Dict, Optional, Set, Any
# import json
# import pymysql
# import pandas as pd
# import numpy as np
# import math
# import time
# import scipy.stats as stats
# from statsmodels.stats.multitest import multipletests
# from cv import cv_calculation
# from std import std_calculation
# from MAD import mad_calculation
# from DEPTH2 import depth2_calculation
# from DEPTH_ITH import depth_calculation
# from cv_2 import cv2_calculation
# from mean import mean_calculation
# from pydantic import BaseModel
# import gseapy as gp 
# import requests
# import logging
# import io

# # === LOGGING CONFIG ===
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # === FASTAPI CONFIG ===
# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # === DATABASE CONFIG ===
# DB_CONFIG = {
#     "host": "localhost",
#     "user": "rubab",
#     "password": "initiate123",
#     "database": "cancer_db",
#     "cursorclass": pymysql.cursors.DictCursor,
# }

# # === METRIC FUNCTIONS ===
# metric_funcs_gene = {
#     "cv": cv_calculation,
#     "cv_squared": cv2_calculation,
#     "std": std_calculation,
#     "mad": mad_calculation,
#     "mean": mean_calculation,
# }

# metric_funcs_TH = {
#     "DEPTH2": depth2_calculation,
#     "tITH": depth_calculation,
# }

# def sanitize_floats(obj):
#     """Recursively replace NaN, inf, and -inf with None for JSON serialization."""
#     if isinstance(obj, dict):
#         return {k: sanitize_floats(v) for k, v in obj.items()}
#     elif isinstance(obj, list):
#         return [sanitize_floats(v) for v in obj]
#     elif isinstance(obj, float):
#         if math.isnan(obj) or math.isinf(obj):
#             return None
#         return obj
#     return obj

# def get_connection():
#     return pymysql.connect(**DB_CONFIG)

# class GeneRequest(BaseModel):
#     genes: List[str]  # Accepts a list of genes

# # Pydantic model for response
# class Pathway(BaseModel):
#     value: str
#     label: str
#     category: str  # Added to include category information

# @app.post("/api/enriched-pathways", response_model=List[Pathway])
# async def get_enriched_pathways(request: GeneRequest):
#     try:
#         # STRINGdb API endpoint for functional enrichment
#         response = requests.post(
#             "https://string-db.org/api/json/enrichment",
#             data={
#                 "identifiers": "\n".join(request.genes),  # Join genes with newlines
#                 "species": 9606,  # Human species
#                 "caller_identity": "your-app-name",  # Replace with your app identifier
#             },
#             timeout=10,
#         )
#         response.raise_for_status()  # Raise an error for bad status codes

#         # Process API response, include all categories
#         pathways = [
#             {
#                 "value": item["term"],
#                 "label": f"{item['description']} (FDR: {item['fdr']}, Category: {item['category']})",
#                 "category": item["category"]
#             }
#             for item in response.json()
#         ]

#         return pathways

#     except requests.RequestException as e:
#         raise HTTPException(status_code=500, detail=f"Failed to fetch enriched pathways: {str(e)}")

# def compute_metrics(values: pd.Series):
#     """Compute mean, std, mad, cv, and cv² for a numeric series."""
#     if len(values) == 0:
#         return None
#     mean = values.mean()
#     std = values.std(ddof=1)
#     mad = (values - mean).abs().mean()
#     cv = (std / mean) * 100 if mean != 0 else np.nan
#     cv2 = cv**2 if not np.isnan(cv) else np.nan
#     return {"mean": mean, "std": std, "mad": mad, "cv": cv, "cv_squared": cv2}

# def perform_ora(genes: List[str], pathway_genes: Dict[str, List[str]], background_genes: List[str]):
#     """Perform Over-Representation Analysis (ORA) for given genes."""
#     results = []
#     for term, pathway_gene_list in pathway_genes.items():
#         # Intersection of input genes and pathway genes
#         overlap = set(genes) & set(pathway_gene_list)
#         if not overlap:
#             continue

#         # Contingency table for hypergeometric test
#         k = len(overlap)  # Number of successes (genes in pathway)
#         M = len(background_genes)  # Total population
#         n = len(pathway_gene_list)  # Number of successes in population
#         N = len(genes)  # Number of draws

#         # Hypergeometric test
#         p_value = stats.hypergeom.sf(k - 1, M, n, N)
#         odds_ratio, _ = stats.fisher_exact([[k, n - k], [N - k, M - n - (N - k)]], alternative='greater')

#         results.append({
#             "Term": term,
#             "P-value": p_value,
#             "Genes": list(overlap),
#             "Overlap": f"{len(overlap)}/{len(pathway_gene_list)}",
#             "Odds Ratio": odds_ratio,
#             "GeneSet": term,
#         })

#     # Adjust p-values
#     if results:
#         p_values = [r["P-value"] for r in results]
#         adjusted_p = multipletests(p_values, method='fdr_bh')[1]
#         for i, r in enumerate(results):
#             r["Adjusted P-value"] = adjusted_p[i]
#             r["Combined Score"] = -math.log10(r["Adjusted P-value"]) * r["Odds Ratio"] if r["Adjusted P-value"] > 0 else 0

#     return sorted(results, key=lambda x: x["Adjusted P-value"])

# @app.get("/api/genes")
# def get_genes():
#     """Fetch all available gene symbols."""
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
#     try:
#         cur.execute("SELECT DISTINCT gene_symbol FROM genes WHERE gene_symbol IS NOT NULL ORDER BY gene_symbol ASC")
#         rows = cur.fetchall()
#         genes = [row["gene_symbol"] for row in rows]
#         return {"genes": genes}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
#     finally:
#         conn.close()

# @app.get("/api/sites")
# def get_sites():
#     """Fetch all available cancer sites."""
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
#     try:
#         cur.execute("SELECT id, name FROM sites ORDER BY name ASC")
#         sites = cur.fetchall()
#         return JSONResponse({"sites": sites})
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
#     finally:
#         conn.close()

# @app.get("/api/cancer_types")
# def get_cancer_types(site_ids: List[int] = Query(..., description="List of site IDs")):
#     """Fetch cancer types for given site IDs."""
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
#     try:
#         cur.execute(
#             "SELECT tcga_code, site_id FROM cancer_types WHERE site_id IN %s",
#             (tuple(site_ids),),
#         )
#         cancer_types = cur.fetchall()
#         return JSONResponse({"cancer_types": cancer_types})
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
#     finally:
#         conn.close()

# @app.get("/api/gene_noise")
# def get_gene_noise(
#     cancer_site: list[str] = Query(..., description="One or more cancer sites (e.g., Lung, Liver, Adrenal Gland)"),
#     cancer_type: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
#     gene_ids: list[str] = Query(..., description="One or more gene IDs or symbols (e.g., TP53, EGFR)")
# ):
#     """
#     Compute expression noise metrics for one or more cancer sites and genes.
#     Returns JSON with structure:
#     {
#       site: {
#         gene_symbol: {
#           norm_method: { "raw": {...}, "log2": {...} }
#         }
#       }
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)

#     results = {}
#     debug_info = {}
#     sample_counts = {}

#     try:
#         for site in cancer_site:
#             cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#             site_row = cur.fetchone()
#             if not site_row:
#                 debug_info[site] = {"error": f"Site '{site}' not found."}
#                 continue
#             site_id = site_row["id"]

#             # Determine all cancer types for this site
#             if cancer_type:
#                 cur.execute(
#                     "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                     (site_id, tuple(cancer_type)),
#                 )
#             else:
#                 cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#             cancer_types = [row["tcga_code"] for row in cur.fetchall()]

#             if not cancer_types:
#                 debug_info[site] = {"error": f"No cancer types found for site '{site}'."}
#                 continue

#             # Count tumor/normal samples for the site
#             cur.execute(
#                 """
#                 SELECT s.sample_type, COUNT(*) AS count
#                 FROM samples s
#                 JOIN cancer_types c ON c.id = s.cancer_type_id
#                 WHERE c.site_id = %s AND c.tcga_code IN %s
#                 GROUP BY s.sample_type
#                 """,
#                 (site_id, tuple(cancer_types)),
#             )
#             site_counts = {"tumor": 0, "normal": 0}
#             for row in cur.fetchall():
#                 stype = row["sample_type"].lower()
#                 if stype in site_counts:
#                     site_counts[stype] = row["count"]
#             sample_counts[site] = site_counts

#             # Initialize results for this site
#             results[site] = {}

#             # Loop over all requested genes
#             for gene_input in gene_ids:
#                 cur.execute(
#                     "SELECT id, ensembl_id, gene_symbol FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
#                     (gene_input, gene_input),
#                 )
#                 gene_row = cur.fetchone()
#                 if not gene_row:
#                     debug_info.setdefault(site, {})[gene_input] = {"error": f"Gene '{gene_input}' not found."}
#                     continue

#                 gene_id = gene_row["id"]
#                 gene_symbol = gene_row["gene_symbol"]
#                 ensembl_id = gene_row["ensembl_id"]

#                 # Prepare gene entry
#                 results[site][gene_symbol] = {}

#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     all_tumor_vals, all_normal_vals = [], []

#                     for ct in cancer_types:
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.{norm_method} AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene_id),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             continue
#                         df = pd.DataFrame(rows)
#                         all_tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"].tolist())
#                         all_normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"].tolist())

#                     tumor = pd.Series(all_tumor_vals)
#                     normal = pd.Series(all_normal_vals)

#                     tumor_stats = compute_metrics(tumor)
#                     normal_stats = compute_metrics(normal)
#                     logfc_raw = np.nan
#                     if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                         logfc_raw = math.log2(tumor_stats["cv"] / normal_stats["cv"])

#                     # Log2-transformed
#                     df_all = pd.DataFrame({
#                         "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
#                         "expr": list(tumor) + list(normal),
#                     })
#                     df_all["log2_expr"] = np.log2(df_all["expr"] + 1)

#                     tumor_log = df_all[df_all["sample_type"] == "tumor"]["log2_expr"]
#                     normal_log = df_all[df_all["sample_type"] == "normal"]["log2_expr"]

#                     tumor_stats_log = compute_metrics(tumor_log)
#                     normal_stats_log = compute_metrics(normal_log)
#                     logfc_log = np.nan
#                     if tumor_stats_log and normal_stats_log:
#                         logfc_log = tumor_stats_log["cv"] - normal_stats_log["cv"]

#                     results[site][gene_symbol][norm_method] = {
#                         "raw": {
#                             **(tumor_stats or {}),
#                             **{f"{k}_normal": v for k, v in (normal_stats or {}).items()},
#                             "logfc": logfc_log,
#                         },
#                         "log2": {
#                             **(tumor_stats_log or {}),
#                             **{f"{k}_normal": v for k, v in (normal_stats_log or {}).items()},
#                             "logfc": logfc_log,
#                         },
#                     }
#                     print(logfc_raw, logfc_log)

#         final_output = {
#             "results": sanitize_floats(results),
#             "sample_counts": sample_counts,
#         }
#         print(final_output)
#     # print(final_output)
#     except Exception as e:
#         final_output = {"error": f"Unexpected error: {str(e)}"}
#     finally:
#         conn.close()

#     # Clean up NaN, inf, etc. for JSON compliance
#     results = sanitize_floats(final_output)
#     print(results)
#     return JSONResponse(results)



# @app.get("/api/pathway-analysis")
# def get_pathway_analysis(
#     cancer: str = Query(..., description="Comma-separated cancer sites (e.g., Lung,Liver)"),
#     genes: List[str] = Query(None, description="Optional gene symbols (e.g., TP53,EGFR)"),
#     top_n: int = Query(50, description="Number of top pathways or neighbors"),
#     logfc_threshold: float = Query(0.7, description="LogFC threshold"),
#     mode: str = Query("enrichment", description="STRING mode: enrichment or neighbors ('enrichment'|'neighbors')"),
#     network_type: str = Query("functional", description="Network type for neighbors: functional or physical"),
#     score_threshold: float = Query(0.4, description="STRING confidence threshold (0–1)"),
#     pathway: Optional[str] = Query(None, description="Selected pathway term to filter results"),
# ):
#     conn = get_connection()
#     cur = conn.cursor()
    
#     try:
#         # Parse inputs
#         cancer_sites = [s.strip() for s in cancer.split(",") if s.strip()]
        
#         # Get genes
#         gene_list, selected_gene_set, warning = get_genes(cur, genes, cancer_sites, logfc_threshold)
        
#         # If a pathway is specified, fetch its matching genes from STRINGdb
#         if pathway:
#             response = requests.get(
#                 f"https://string-db.org/api/json/enrichment?identifiers={'%0D'.join(genes)}&species=9606"
#             )
#             response.raise_for_status()
#             data = response.json()
#             pathway_data = next((item for item in data if item["term"] == pathway), None)
#             if pathway_data:
#                 selected_gene_set = set(pathway_data["inputGenes"])
#                 cur.execute(
#                     "SELECT id, gene_symbol FROM genes WHERE gene_symbol IN %s",
#                     (tuple(selected_gene_set),)
#                 )
#                 gene_list = [row["id"] for row in cur.fetchall()]
#                 selected_gene_set = set(row["gene_symbol"] for row in cur.fetchall())
#             else:
#                 raise HTTPException(status_code=404, detail=f"Pathway '{pathway}' not found.")
        
#         # Get sample counts
#         sample_counts = get_sample_counts(cur, cancer_sites)
        
#         # Compute gene stats
#         results = compute_gene_stats(cur, cancer_sites, gene_list)
        
#         # Run STRING logic
#         if mode == "neighbors":
#             string_results = run_stringdb_neighbors(selected_gene_set, network_type, score_threshold, top_n)
#             for scale in ["raw", "log2"]:
#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     results[scale][norm_method]["network"] = string_results
#         else:
#             string_results = run_stringdb_enrichment(selected_gene_set, top_n)
#             for scale in ["raw", "log2"]:
#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     results[scale][norm_method]["enrichment"] = string_results
        
#         # Assign top genes
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)
        
#         # Build response
#         output = {
#             "raw": results["raw"],
#             "log2": results["log2"],
#             "sample_counts": sample_counts,
#             "available_sites": get_available_sites(cur),
#             "warning": warning,
#             "mode": mode,
#         }
#         return sanitize_floats(output)
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"Error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()
# def run_stringdb_enrichment(gene_set: Set[str], top_n: int = 50, species: int = 9606) -> List[Dict[str, Any]]:
#     """Run STRINGdb enrichment for a given gene set (only curated databases, no limit)."""
#     if not gene_set:
#         return []

#     try:
#         identifiers = "%0D".join(list(gene_set))
#         url = f"https://string-db.org/api/json/enrichment?identifiers={identifiers}&species={species}"
        
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()

#         if not data:
#             return []

#         # Map STRING categories to simplified database names
#         category_map = {
#             "Process": "GO:BP",
#             "Function": "GO:MF",
#             "Component": "GO:CC",
#             "KEGG": "KEGG",
#             "Reactome": "Reactome",
#             "UniProt": "UniProt",
#             "WikiPathways": "WikiPathways"
#         }

#         # Keep only curated databases (exclude PubMed/literature-based sets)
#         valid_categories = {"GO:BP", "GO:MF", "GO:CC", "KEGG", "Reactome", "UniProt", "WikiPathways"}

#         # Map and filter results
#         mapped = []
#         for item in data:
#             db = category_map.get(item.get("category", "NA"), "Unknown")
#             if db in valid_categories:
#                 mapped.append({**item, "Database": db})

#         # Sort by FDR (ascending) but no limit applied
#         enriched = sorted(mapped, key=lambda x: x.get("fdr", 1))

#         # Build clean result structure
#         results = [{
#             "Term": item.get("term", "NA"),
#             "Database": item.get("Database", "Unknown"),
#             "FDR": item.get("fdr", 1.0),
#             "MatchingGenes": item.get("inputGenes", []),
#             "Description": item.get("description", "NA"),
#             "GeneCount": len(item.get("inputGenes", [])),
#             "EnrichmentScore": -math.log10(item.get("fdr", 1e-10)),
#             "GeneSet": "STRINGdb"
#         } for item in enriched]

#         return results

#     except Exception as e:
#         print(f"STRING enrichment failed: {e}")
#         return []


# def run_stringdb_neighbors(gene_set: Set[str], network_type: str = "functional", score_threshold: float = 0.4, top_n: int = 50, species: int = 9606) -> List[Dict[str, Any]]:
#     """Get nearest neighbor network from STRINGdb."""
#     if not gene_set:
#         return []
    
#     try:
#         identifiers = "%0D".join(list(gene_set))
#         url = f"https://string-db.org/api/json/network?identifiers={identifiers}&species={species}"
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()

#         if not data:
#             return []

#         # Filter by confidence score
#         filtered = [x for x in data if x.get("score", 0) >= score_threshold]

#         if network_type == "physical":
#             filtered = [x for x in filtered if x.get("network_type", "functional") == "physical"]

#         # Limit to top N by confidence
#         filtered = sorted(filtered, key=lambda x: x.get("score", 0), reverse=True)[:top_n]

#         results = [{
#             "source": x.get("preferredName_A"),
#             "target": x.get("preferredName_B"),
#             "score": x.get("score"),
#         } for x in filtered]

#         return results

#     except Exception as e:
#         print(f"STRING neighbors failed: {e}")
#         return []

# # === HELPER FUNCTIONS ===

# def get_genes(cur, genes_input: List[str], cancer_sites: List[str], logfc_threshold: float) -> tuple[List[int], Set[str], str]:
#     """Get gene list from input or auto-select top DE genes."""
#     if genes_input:
#         all_genes = []
#         for gene_input in genes_input:
#             all_genes.extend([g.strip().upper() for g in str(gene_input).split(",") if g.strip()])
        
#         unique_genes = list(dict.fromkeys(all_genes))
        
#         gene_list = []
#         gene_set = set()
#         debug = {}
        
#         for gene_symbol in unique_genes:
#             cur.execute("SELECT id, gene_symbol FROM genes WHERE gene_symbol = %s OR ensembl_id = %s", 
#                        (gene_symbol, gene_symbol))
#             row = cur.fetchone()
#             if row:
#                 gene_list.append(row["id"])
#                 gene_set.add(row["gene_symbol"])
#             else:
#                 debug[gene_symbol] = "not found"
        
#         warning = f"Found {len(gene_list)}/{len(unique_genes)} genes" if debug else None
#         return gene_list, gene_set, warning
    
#     warning = "No genes provided. Using top DE genes."
#     gene_list = []
#     gene_set = set()
    
#     for site in cancer_sites[:2]:
#         cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#         site_row = cur.fetchone()
#         if not site_row:
#             continue
            
#         cur.execute("""
#             SELECT g.id, g.gene_symbol, AVG(CASE WHEN s.sample_type = 'Tumor' THEN ge.tpm END) as tumor_mean,
#                    AVG(CASE WHEN s.sample_type = 'Normal' THEN ge.tpm END) as normal_mean
#             FROM genes g
#             JOIN gene_expressions ge ON g.id = ge.gene_id
#             JOIN samples s ON ge.sample_id = s.id
#             JOIN cancer_types c ON s.cancer_type_id = c.id
#             WHERE c.site_id = %s
#             GROUP BY g.id, g.gene_symbol
#             HAVING normal_mean > 0 AND tumor_mean > 0
#             ORDER BY ABS(LOG2(tumor_mean/normal_mean)) DESC
#             LIMIT 10
#         """, (site_row["id"],))
        
#         for row in cur.fetchall():
#             gene_list.append(row["id"])
#             gene_set.add(row["gene_symbol"])
    
#     return list(set(gene_list)), gene_set, warning

# def get_sample_counts(cur, cancer_sites: List[str]) -> Dict[str, Dict[str, int]]:
#     """Get tumor/normal sample counts per site."""
#     counts = {}
#     for site in cancer_sites:
#         cur.execute("""
#             SELECT s.sample_type, COUNT(*) as count
#             FROM samples s
#             JOIN cancer_types c ON s.cancer_type_id = c.id
#             JOIN sites si ON c.site_id = si.id
#             WHERE si.name = %s
#             GROUP BY s.sample_type
#         """, (site,))
        
#         site_counts = {"tumor": 0, "normal": 0}
#         for row in cur.fetchall():
#             stype = row["sample_type"].lower()
#             if stype in site_counts:
#                 site_counts[stype] = row["count"]
#         counts[site] = site_counts
#     return counts

# def compute_gene_stats(cur, cancer_sites: List[str], gene_list: List[int]) -> Dict[str, Dict[str, Dict[str, Any]]]:
#     """Compute stats for all genes, norm methods, and scales."""
#     results = {"raw": {}, "log2": {}}
    
#     for scale in ["raw", "log2"]:
#         for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#             results[scale][norm_method] = {
#                 "enrichment": [],
#                 "network": [],
#                 "top_genes": [],
#                 "gene_stats": {},
#                 "heatmap_data": {},
#                 "noise_metrics": {},
#             }
    
#     for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#         gene_stats_raw = {}
#         heatmap_data = {}
#         noise_metrics = {}
        
#         for gene_id in gene_list:
#             gene_symbol = get_gene_symbol(cur, gene_id)
            
#             if gene_symbol not in gene_stats_raw:
#                 gene_stats_raw[gene_symbol] = {}
#                 heatmap_data[gene_symbol] = {}
#                 noise_metrics[gene_symbol] = {}
            
#             for site in cancer_sites:
#                 stats = get_gene_stats_for_site(cur, site, gene_id, norm_method)
#                 site_key = site.lower()
                
#                 gene_stats_raw[gene_symbol][site_key] = stats
#                 heatmap_data[gene_symbol][f"{site}_normal"] = stats.get("mean_normal", 0)
#                 heatmap_data[gene_symbol][f"{site}_tumor"] = stats.get("mean_tumor", 0)
#                 noise_metrics[gene_symbol][site_key] = {
#                     "cv_tumor": stats.get("cv_tumor", 0),
#                     "cv_normal": stats.get("cv_normal", 0),
#                     "logfc": stats.get("logfc", np.nan)
#                 }
        
#         for gene_symbol in gene_stats_raw:
#             results["raw"][norm_method]["gene_stats"][gene_symbol] = gene_stats_raw[gene_symbol]
#             results["raw"][norm_method]["heatmap_data"][gene_symbol] = heatmap_data[gene_symbol]
#             results["raw"][norm_method]["noise_metrics"][gene_symbol] = noise_metrics[gene_symbol]
        
#         results["log2"][norm_method]["gene_stats"] = {k: v.copy() for k, v in gene_stats_raw.items()}
#         results["log2"][norm_method]["heatmap_data"] = {k: v.copy() for k, v in heatmap_data.items()}
#         results["log2"][norm_method]["noise_metrics"] = {k: v.copy() for k, v in noise_metrics.items()}
    
#     return results

# def get_gene_stats_for_site(cur, site: str, gene_id: int, norm_method: str) -> Dict[str, float]:
#     """Get stats for one gene/site/norm_method."""
#     cur.execute("""
#         SELECT s.sample_type, e.{norm} as expr
#         FROM gene_expressions e
#         JOIN samples s ON e.sample_id = s.id
#         JOIN cancer_types c ON s.cancer_type_id = c.id
#         JOIN sites si ON c.site_id = si.id
#         WHERE si.name = %s AND e.gene_id = %s
#     """.format(norm=norm_method), (site, gene_id))
    
#     rows = cur.fetchall()
#     if not rows:
#         return {}
    
#     df = pd.DataFrame(rows)
#     tumor = df[df["sample_type"].str.lower() == "tumor"]["expr"]
#     normal = df[df["sample_type"].str.lower() == "normal"]["expr"]
    
#     tumor_stats = compute_metrics(tumor)
#     normal_stats = compute_metrics(normal)
    
#     return {
#         "mean_tumor": tumor_stats["mean"] if tumor_stats else None,
#         "mean_normal": normal_stats["mean"] if normal_stats else None,
#         "cv_tumor": tumor_stats["cv"] if tumor_stats else None,
#         "cv_normal": normal_stats["cv"] if normal_stats else None,
#         "logfc": compute_logfc(tumor_stats, normal_stats)
#     }

# def get_available_sites(cur) -> List[Dict[str, Any]]:
#     cur.execute("SELECT id, name FROM sites ORDER BY name")
#     return cur.fetchall()

# def get_gene_symbol(cur, gene_id: int) -> str:
#     cur.execute("SELECT gene_symbol FROM genes WHERE id = %s", (gene_id,))
#     row = cur.fetchone()
#     return row["gene_symbol"] if row else f"gene_{gene_id}"

# def compute_logfc(tumor_stats: Dict[str, float], normal_stats: Dict[str, float]) -> float:
#     if not tumor_stats or not normal_stats or normal_stats["cv"] == 0:
#         return np.nan
#     return math.log2(tumor_stats["cv"] / normal_stats["cv"])

# @app.get("/api/tumor_results")
# def get_tumor_results(
#     cancer_site: str = Query(..., description="Cancer site (e.g., Lung, Liver)"),
#     cancer_types: List[str] = Query(None, description="Optional list of cancer types (TCGA codes)")
# ):
#     """
#     Fetch tumor sample data including DEPTH2 and tITH scores for a given cancer site and optional cancer types.
#     Returns JSON with structure:
#     {
#       "results": [
#         {
#           "sample_id": int,
#           "sample_barcode": str,
#           "cancer_type": str,
#           "tpm_depth2": float|null,
#           "fpkm_depth2": float|null,
#           "fpkm_uq_depth2": float|null,
#           "tpm_tith": float|null,
#           "fpkm_tith": float|null,
#           "fpkm_uq_tith": float|null
#         }
#       ],
#       "sample_counts": {
#         "tumor": int,
#         "normal": int
#       },
#       "error": str|null
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
    
#     try:
#         # Fetch site ID
#         cur.execute("SELECT id FROM sites WHERE name = %s", (cancer_site,))
#         site_row = cur.fetchone()
#         if not site_row:
#             raise HTTPException(status_code=404, detail=f"Cancer site '{cancer_site}' not found.")
#         site_id = site_row["id"]

#         # Fetch cancer types
#         if cancer_types:
#             cur.execute(
#                 "SELECT id, tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                 (site_id, tuple(cancer_types)),
#             )
#         else:
#             cur.execute("SELECT id, tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#         cancer_type_rows = cur.fetchall()
#         if not cancer_type_rows:
#             raise HTTPException(status_code=404, detail=f"No cancer types found for site '{cancer_site}'.")
        
#         cancer_type_ids = [row["id"] for row in cancer_type_rows]
#         cancer_type_map = {row["id"]: row["tcga_code"] for row in cancer_type_rows}

#         # Fetch sample counts
#         cur.execute(
#             """
#             SELECT s.sample_type, COUNT(*) AS count
#             FROM samples s
#             JOIN cancer_types c ON c.id = s.cancer_type_id
#             WHERE c.site_id = %s AND c.tcga_code IN %s
#             GROUP BY s.sample_type
#             """,
#             (site_id, tuple(cancer_type_map.values())),
#         )
#         sample_counts = {"tumor": 0, "normal": 0}
#         for row in cur.fetchall():
#             stype = row["sample_type"].lower()
#             if stype in sample_counts:
#                 sample_counts[stype] = row["count"]

#         # Fetch tumor samples with DEPTH2 and tITH scores
#         cur.execute(
#             """
#             SELECT 
#                 s.id AS sample_id,
#                 s.sample_barcode,
#                 s.cancer_type_id,
#                 d.tpm AS tpm_depth2,
#                 d.fpkm AS fpkm_depth2,
#                 d.fpkm_uq AS fpkm_uq_depth2,
#                 t.tpm AS tpm_tith,
#                 t.fpkm AS fpkm_tith,
#                 t.fpkm_uq AS fpkm_uq_tith
#             FROM samples s
#             JOIN cancer_types c ON c.id = s.cancer_type_id
#             LEFT JOIN depth2_scores d ON d.sample_id = s.id
#             LEFT JOIN tith_scores t ON t.sample_id = s.id
#             WHERE c.site_id = %s 
#             AND c.id IN %s 
#             AND s.sample_type = 'tumor'
#             """,
#             (site_id, tuple(cancer_type_ids)),
#         )
#         rows = cur.fetchall()

#         results = [
#             {
#                 "sample_id": row["sample_id"],
#                 "sample_barcode": row["sample_barcode"],
#                 "cancer_type": cancer_type_map.get(row["cancer_type_id"], "Unknown"),
#                 "tpm_depth2": row["tpm_depth2"],
#                 "fpkm_depth2": row["fpkm_depth2"],
#                 "fpkm_uq_depth2": row["fpkm_uq_depth2"],
#                 "tpm_tith": row["tpm_tith"],
#                 "fpkm_tith": row["fpkm_tith"],
#                 "fpkm_uq_tith": row["fpkm_uq_tith"],
#             }
#             for row in rows
#         ]

#         final_output = {
#             "results": results,
#             "sample_counts": sample_counts,
#             "error": None
#         }
#         return JSONResponse(sanitize_floats(final_output))

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
#     finally:
#         conn.close()

# @app.post("/api/csv-upload")
# async def csv_upload(request: Request):
#     try:
#         form = await request.form()
#         analysis_type = form.get('analysis_type', 'Pathway')
#         top_n = int(form.get('top_n', 15))
#         gene_set = form.get('gene_set', 'KEGG_2021_Human') if analysis_type == 'Pathway' else None
#         genes = form.get('genes', '')

#         logger.info(f"Input: analysis_type={analysis_type}, top_n={top_n}, gene_set={gene_set}, genes={genes}")

#         valid_analysis_types = ['Gene', 'Pathway', 'Tumor']
#         if analysis_type not in valid_analysis_types:
#             return JSONResponse({"error": f"Invalid analysis type: {analysis_type}"}, status_code=400)

#         selected_genes = [g.strip().upper() for g in genes.split(',') if g.strip()] if genes.strip() else None
#         if analysis_type in ['Gene', 'Pathway'] and not selected_genes and 'expression_file' not in form:
#             return JSONResponse({"error": "Gene or Pathway Analysis requires a gene list or expression data"}, status_code=400)

#         # Initialize response
#         response = {
#             "analysis_type": analysis_type,
#             "raw": {"warning": "Differential noise analysis (e.g., logFC) is not possible"},
#             "log2": {"warning": "Differential noise analysis (e.g., logFC) is not possible"}
#         } if analysis_type != 'Tumor' else {
#             "analysis_type": analysis_type,
#             "log2": {"warning": ""}
#         }

#         # Process uploaded expression file
#         df_raw = None
#         df_log2 = None
#         if 'expression_file' in form:
#             expression_file = form['expression_file']
#             if not expression_file.filename:
#                 return JSONResponse({"error": "No expression file selected"}, status_code=400)
#             if not expression_file.filename.endswith(('.csv', '.tsv')):
#                 return JSONResponse({"error": "Expression file must be CSV or TSV"}, status_code=400)
#             delimiter = ',' if expression_file.filename.endswith('.csv') else '\t'

#             # Read the CSV/TSV file
#             content = await expression_file.read()
#             df_raw = pd.read_csv(io.BytesIO(content), delimiter=delimiter)
#             if not {'Hugo_Symbol', 'gene_name'}.intersection(df_raw.columns):
#                 return JSONResponse({"error": "Expression file must contain 'Hugo_Symbol' or 'gene_name' column"}, status_code=400)

#             gene_column = 'gene_name' if 'gene_name' in df_raw.columns else 'Hugo_Symbol'
#             gene_id = 'gene_id' if 'gene_id' in df_raw.columns else 'Entrez_Gene_Id' if 'Entrez_Gene_Id' in df_raw.columns else None
#             df_raw[gene_column] = df_raw[gene_column].astype(str).str.upper()

#             # Prepare raw and log2 data
#             df_raw = df_raw.set_index(gene_column)
#             if gene_id in df_raw.columns:
#                 df_raw = df_raw.drop(columns=[gene_id])
#             df_raw = df_raw.apply(pd.to_numeric, errors='coerce')
#             df_raw = df_raw.fillna(df_raw.median(numeric_only=True))
#             df_log2 = np.log2(df_raw + 1)

#         # Handle Gene Analysis
#         if analysis_type == 'Gene':
#             for transform, df in [('raw', df_raw), ('log2', df_log2)]:
#                 if df is not None:
#                     top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
#                     if not top_genes:
#                         return JSONResponse({"error": f"None of the provided genes found in {transform} expression data"}, status_code=400)
#                     df = df.loc[top_genes]
#                     metrics = {name: func(df).to_dict() for name, func in metric_funcs_gene.items()}
#                 else:
#                     if not selected_genes:
#                         return JSONResponse({"error": "Gene Analysis requires a gene list or expression data"}, status_code=400)
#                     top_genes = selected_genes
#                     metrics = {name: {gene: 0.0 for gene in top_genes} for name in metric_funcs_gene.keys()}
#                 response[transform].update({"metrics": metrics, "top_genes": top_genes})

#         # Handle Pathway Analysis
#         elif analysis_type == 'Pathway':
#             for transform, df in [('raw', df_raw), ('log2', df_log2)]:
#                 if df is not None:
#                     top_genes = df.index.tolist() if selected_genes is None else [g for g in selected_genes if g in df.index]
#                     if not top_genes:
#                         return JSONResponse({"error": f"None of the provided genes found in {transform} expression data"}, status_code=400)
#                     df = df.loc[top_genes]
#                     cv_values = metric_funcs_gene['cv'](df)
#                     heatmap_data = {gene: {"mean": float(df.loc[gene].mean()), "cv": float(cv_values.get(gene, 0.0))} for gene in top_genes}
#                 else:
#                     if not selected_genes:
#                         return JSONResponse({"error": "Pathway Analysis requires a gene list or expression data"}, status_code=400)
#                     top_genes = selected_genes
#                     heatmap_data = {gene: {"mean": 0.0, "cv": 0.0} for gene in top_genes}

#                 enrichment_results = []
#                 try:
#                     enr = gp.enrichr(gene_list=top_genes, gene_sets=gene_set, organism="Human", outdir=None, cutoff=0.05)
#                     results_df = enr.results
#                     if not results_df.empty:
#                         results = results_df[["Term", "P-value", "Adjusted P-value", "Odds Ratio", "Combined Score", "Genes"]].head(top_n).to_dict(orient="records")
#                         for res in results:
#                             res["Genes"] = res["Genes"].split(";") if isinstance(res["Genes"], str) else []
#                             res["GeneSet"] = gene_set
#                         enrichment_results.extend(results)
#                 except Exception as e:
#                     logger.error(f"Failed to run ORA with {gene_set} for {transform}: {e}")

#                 response[transform].update({
#                     "enrichment": enrichment_results,
#                     "top_genes": top_genes,
#                     "heatmap_data": heatmap_data
#                 })

#         # Handle Tumor Analysis
#         elif analysis_type == 'Tumor':
#             if df_log2 is None:
#                 return JSONResponse({"error": "Tumor Analysis requires an expression data file"}, status_code=400)
#             all_metrics = {}
#             for metric_name, func in metric_funcs_TH.items():
#                 try:
#                     metric_series = func(df_log2)  # Compute across all samples
#                     all_metrics[metric_name] = metric_series
#                 except Exception as e:
#                     logger.error(f"Failed to compute {metric_name}: {e}")
#                     all_metrics[metric_name] = pd.Series(0.0, index=df_log2.columns)

#             metrics = []
#             for sample in df_log2.columns:
#                 sample_metrics = {"sample": sample}
#                 for metric_name, metric_series in all_metrics.items():
#                     val = metric_series.get(sample, 0.0)
#                     sample_metrics[metric_name] = float(val) if pd.notna(val) else 0.0
#                 metrics.append(sample_metrics)
#             response["log2"]["metrics"] = metrics

#         logger.info(f"CSV upload response generated for {analysis_type} analysis")
#         print(response)
#         return JSONResponse(response)

#     except Exception as e:
#         logger.error(f"CSV upload failed: {e}")
#         return JSONResponse({"error": f"Internal server error: {str(e)}"}, status_code=500)

# # ------------------------------
# # Entry Point
# # ------------------------------
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=5001)




# @app.get("/api/pathway-analysis")
# def get_pathway_analysis(
#     cancer: str = Query(..., description="Comma-separated cancer sites (e.g., Lung,Liver)"),
#     genes: list[str] = Query(None, description="Optional gene symbols (e.g., TP53,EGFR)"),
#     top_n: int = Query(15, description="Number of top pathways"),
#     logfc_threshold: float = Query(0.7, description="LogFC threshold"),
#     library: str = Query("KEGG_2021_Human", description="Gene set library")
# ):
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
    
#     try:
#         # 1. Parse inputs
#         cancer_sites = [s.strip() for s in cancer.split(",") if s.strip()]
#         print(f"Processing sites: {cancer_sites}")
        
#         # 2. Get genes
#         gene_list, selected_gene_set, warning = get_genes(cur, genes, cancer_sites, logfc_threshold)
#         print(f"Selected genes: {list(selected_gene_set)}")
        
#         # 3. Validate library
#         if library not in VALID_LIBRARIES:
#             raise HTTPException(status_code=400, detail=f"Invalid library: {library}")
        
#         # 4. Get sample counts
#         sample_counts = get_sample_counts(cur, cancer_sites)
        
#         # 5. Compute gene stats for all norm methods
#         results = compute_gene_stats(cur, cancer_sites, gene_list)
        
#         # 6. Run enrichment (same for all)
#         enrichment_results = run_enrichment(selected_gene_set, library, top_n)
        
#         # 7. Assign enrichment to all results
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method]["enrichment"] = enrichment_results
#                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)
        
#         # 8. Build response
        
#         results = {
#             "raw": results["raw"],
#             "log2": results["log2"],
#             "sample_counts": sample_counts,
#             "available_sites": get_available_sites(cur),
#             "warning": warning,
#         }
#         print(results)
#         return sanitize_floats(results)
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"Error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()


# # === HELPER FUNCTIONS ===

# VALID_LIBRARIES = [
#     "KEGG_2021_Human", "Reactome_2022", 
#     "GO_Biological_Process_2023", "GO_Molecular_Function_2023", "GO_Cellular_Component_2023"
# ]

# def get_genes(cur, genes_input, cancer_sites, logfc_threshold):
#     """Get gene list from input or auto-select top DE genes"""
#     if genes_input:
#         # Parse and validate user genes
#         all_genes = []
#         for gene_input in genes_input:
#             all_genes.extend([g.strip().upper() for g in str(gene_input).split(",") if g.strip()])
        
#         # Remove duplicates
#         unique_genes = list(dict.fromkeys(all_genes))
        
#         gene_list = []
#         gene_set = set()
#         debug = {}
        
#         for gene_symbol in unique_genes:
#             cur.execute("SELECT id, gene_symbol FROM genes WHERE gene_symbol = %s OR ensembl_id = %s", 
#                        (gene_symbol, gene_symbol))
#             row = cur.fetchone()
#             if row:
#                 gene_list.append(row["id"])
#                 gene_set.add(row["gene_symbol"])
#             else:
#                 debug[gene_symbol] = "not found"
        
#         warning = f"Found {len(gene_list)}/{len(unique_genes)} genes" if debug else None
#         return gene_list, gene_set, warning
    
#     # Auto-select top genes
#     warning = "No genes provided. Using top DE genes."
#     gene_list = []
#     gene_set = set()
    
#     for site in cancer_sites[:2]:  # Limit to 2 sites for speed
#         cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#         site_row = cur.fetchone()
#         if not site_row:
#             continue
            
#         # Get top 10 genes per site
#         cur.execute("""
#             SELECT g.id, g.gene_symbol, AVG(CASE WHEN s.sample_type = 'Tumor' THEN ge.tpm END) as tumor_mean,
#                    AVG(CASE WHEN s.sample_type = 'Normal' THEN ge.tpm END) as normal_mean
#             FROM genes g
#             JOIN gene_expressions ge ON g.id = ge.gene_id
#             JOIN samples s ON ge.sample_id = s.id
#             JOIN cancer_types c ON s.cancer_type_id = c.id
#             WHERE c.site_id = %s
#             GROUP BY g.id, g.gene_symbol
#             HAVING normal_mean > 0 AND tumor_mean > 0
#             ORDER BY ABS(LOG2(tumor_mean/normal_mean)) DESC
#             LIMIT 10
#         """, (site_row["id"],))
        
#         for row in cur.fetchall():
#             gene_list.append(row["id"])
#             gene_set.add(row["gene_symbol"])
    
#     return list(set(gene_list)), gene_set, warning

# def get_sample_counts(cur, cancer_sites):
#     """Get tumor/normal sample counts per site"""
#     counts = {}
#     for site in cancer_sites:
#         cur.execute("""
#             SELECT s.sample_type, COUNT(*) as count
#             FROM samples s
#             JOIN cancer_types c ON s.cancer_type_id = c.id
#             JOIN sites si ON c.site_id = si.id
#             WHERE si.name = %s
#             GROUP BY s.sample_type
#         """, (site,))
        
#         site_counts = {"tumor": 0, "normal": 0}
#         for row in cur.fetchall():
#             stype = row["sample_type"].lower()
#             if stype in site_counts:
#                 site_counts[stype] = row["count"]
#         counts[site] = site_counts
#     return counts

# def compute_gene_stats(cur, cancer_sites, gene_list):
#     """Compute stats for all genes, norm methods, and scales"""
#     results = {"raw": {}, "log2": {}}
    
#     for scale in ["raw", "log2"]:
#         for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#             results[scale][norm_method] = {
#                 "enrichment": [],
#                 "top_genes": [],
#                 "gene_stats": {},
#                 "heatmap_data": {},
#                 "noise_metrics": {},
#             }
    
#     for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#         # Initialize EMPTY structures BEFORE site loop
#         gene_stats_raw = {}
#         heatmap_data = {}
#         noise_metrics = {}
        
#         for gene_id in gene_list:
#             gene_symbol = get_gene_symbol(cur, gene_id)
            
#             # Initialize gene dicts ONCE per gene
#             if gene_symbol not in gene_stats_raw:
#                 gene_stats_raw[gene_symbol] = {}
#                 heatmap_data[gene_symbol] = {}
#                 noise_metrics[gene_symbol] = {}
            
#             # NOW accumulate stats for EACH site
#             for site in cancer_sites:
#                 stats = get_gene_stats_for_site(cur, site, gene_id, norm_method)
#                 site_key = site.lower()
                
#                 # ACCUMULATE - don't overwrite!
#                 gene_stats_raw[gene_symbol][site_key] = stats
#                 heatmap_data[gene_symbol][f"{site}_normal"] = stats.get("mean_normal", 0)
#                 heatmap_data[gene_symbol][f"{site}_tumor"] = stats.get("mean_tumor", 0)
#                 noise_metrics[gene_symbol][site_key] = {
#                     "cv_tumor": stats.get("cv_tumor", 0),
#                     "cv_normal": stats.get("cv_normal", 0),
#                     "logfc": stats.get("logfc", np.nan)
#                 }
        
#         # Assign to results
#         for gene_symbol in gene_stats_raw:
#             results["raw"][norm_method]["gene_stats"][gene_symbol] = gene_stats_raw[gene_symbol]
#             results["raw"][norm_method]["heatmap_data"][gene_symbol] = heatmap_data[gene_symbol]
#             results["raw"][norm_method]["noise_metrics"][gene_symbol] = noise_metrics[gene_symbol]
        
#         # Copy to log2 (you'd compute real log2 stats here)
#         results["log2"][norm_method]["gene_stats"] = {k: v.copy() for k, v in gene_stats_raw.items()}
#         results["log2"][norm_method]["heatmap_data"] = {k: v.copy() for k, v in heatmap_data.items()}
#         results["log2"][norm_method]["noise_metrics"] = {k: v.copy() for k, v in noise_metrics.items()}
    
#     return results

# def get_gene_stats_for_site(cur, site, gene_id, norm_method):
#     """Get stats for one gene/site/norm_method (aggregates all cancer types)"""
#     cur.execute("""
#         SELECT s.sample_type, e.{norm} as expr
#         FROM gene_expressions e
#         JOIN samples s ON e.sample_id = s.id
#         JOIN cancer_types c ON s.cancer_type_id = c.id
#         JOIN sites si ON c.site_id = si.id
#         WHERE si.name = %s AND e.gene_id = %s
#     """.format(norm=norm_method), (site, gene_id))
    
#     rows = cur.fetchall()
#     if not rows:
#         return {}
    
#     df = pd.DataFrame(rows)
#     tumor = df[df["sample_type"].str.lower() == "tumor"]["expr"]
#     normal = df[df["sample_type"].str.lower() == "normal"]["expr"]
    
#     tumor_stats = compute_metrics(tumor)
#     normal_stats = compute_metrics(normal)
    
#     return {
#         "mean_tumor": tumor_stats["mean"] if tumor_stats else None,
#         "mean_normal": normal_stats["mean"] if normal_stats else None,
#         "cv_tumor": tumor_stats["cv"] if tumor_stats else None,
#         "cv_normal": normal_stats["cv"] if normal_stats else None,
#         "logfc": compute_logfc(tumor_stats, normal_stats)
#     }

# def run_enrichment(gene_set, library, top_n):
#     """Run Enrichr analysis"""
#     if not gene_set:
#         return []
    
#     try:
#         enr = gp.enrichr(gene_list=list(gene_set), gene_sets=library, organism='human', outdir=None)
#         res = enr.results
        
#         if res.empty:
#             return []
        
#         res = res[res["Adjusted P-value"] <= 0.2].head(top_n)
#         res = res.sort_values("Adjusted P-value")
        
#         return [{
#             "Term": row["Term"],
#             "P-value": row["P-value"],
#             "Adjusted P-value": row["Adjusted P-value"],
#             "Combined Score": row["Combined Score"],
#             "Odds Ratio": row["Odds Ratio"],
#             "Genes": row["Genes"].split(";"),
#             "GeneSet": library,
#             "Overlap": row["Overlap"],
#         } for _, row in res.iterrows()]
#     except Exception as e:
#         print(f"Enrichment failed: {e}")
#         return []

# def get_available_sites(cur):
#     cur.execute("SELECT id, name FROM sites ORDER BY name")
#     return cur.fetchall()

# def get_gene_symbol(cur, gene_id):
#     cur.execute("SELECT gene_symbol FROM genes WHERE id = %s", (gene_id,))
#     row = cur.fetchone()
#     return row["gene_symbol"] if row else f"gene_{gene_id}"

# def compute_logfc(tumor_stats, normal_stats):
#     if not tumor_stats or not normal_stats or normal_stats["cv"] == 0:
#         return np.nan
#     return math.log2(tumor_stats["cv"] / normal_stats["cv"])

# @app.get("/api/pathway-analysis")
# def get_pathway_analysis(
#     cancer: str = Query(..., description="Comma-separated cancer sites or single site (e.g., Lung,Liver or Lung)"),
#     cancer_types: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
#     genes: list[str] = Query(None, description="Optional list of gene IDs or symbols for ORA (e.g., TP53,EGFR)"),
#     top_n: int = Query(15, description="Number of top pathways to return"),
#     # analysis_type: str = Query("ORA", description="Type of analysis: ORA or GSEA"),
#     # site_analysis_type: str = Query("pan-cancer", description="Analysis scope: pan-cancer or cancer-specific"),
#     logfc_threshold: float = Query(0.7, description="LogFC threshold for selecting significant genes"),
#     library: str = Query("KEGG_2021_Human", description="Gene set library for enrichment analysis (e.g., KEGG_2021_Human, Reactome_2022)")
    
# ):
#     print(cancer)
#     """
#     Perform pathway enrichment analysis for specified cancer sites and genes using Enrichr.
#     Returns JSON with structure:
#     {
#       "raw": {
#         norm_method: {
#           "enrichment": [{Term, P-value, Adjusted P-value, Combined Score, Odds Ratio, Genes, GeneSet}],
#           "top_genes": [str],
#           "gene_stats": {gene: {site: {mean_tumor, mean_normal, cv_tumor, cv_normal, logfc}}},
#           "heatmap_data": {gene: {site_normal: float, site_tumor: float}},
#           "noise_metrics": {gene: {site: {cv_tumor, cv_normal, logfc}}}
#         }
#       },
#       "log2": {...},
#       "sample_counts": {site: {tumor: int, normal: int}},
#       "available_sites": [{id: int, name: str}],
#       "warning": str,
#       "debug": dict
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)

#     cancer_sites = [s.strip() for s in cancer.split(",")]
#     print(cancer_sites)
#     results = {"raw": {}, "log2": {}}
#     sample_counts = {}
#     debug_info = {}
#     available_sites = []
#     warning = None
#     print(library)
#     try:
#         # Validate library
#         valid_libraries = [
#             "KEGG_2021_Human",
#             "Reactome_2022",
#             "GO_Biological_Process_2023",
#             "GO_Molecular_Function_2023",
#             "GO_Cellular_Component_2023"
#         ]
#         if library not in valid_libraries:
#             raise HTTPException(status_code=400, detail=f"Invalid library. Must be one of: {', '.join(valid_libraries)}.")

#         # Fetch available sites
#         cur.execute("SELECT id, name FROM sites")
#         available_sites = cur.fetchall()

#         # # Validate analysis type
#         # if analysis_type not in ["ORA", "GSEA"]:
#         #     raise HTTPException(status_code=400, detail="Invalid analysis_type. Must be 'ORA' or 'GSEA'.")
#         # if site_analysis_type not in ["pan-cancer", "cancer-specific"]:
#         #     raise HTTPException(status_code=400, detail="Invalid site_analysis_type. Must be 'pan-cancer' or 'cancer-specific'.")

#         # Initialize results structures
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method] = {
#                     "enrichment": [],
#                     "top_genes": [],
#                     "gene_stats": {},
#                     "heatmap_data": {},
#                     "noise_metrics": {},
#                 }

#         # # Fetch gene info
#         # selected_gene_set = set()
#         # gene_symbols = {}
#         # gene_ids = []
#         # if genes:
#         #     for gene_input in genes:
#         #         cur.execute(
#         #             "SELECT id, gene_symbol, ensembl_id FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
#         #             (gene_input, gene_input),
#         #         )
#         #         gene_row = cur.fetchone()
#         #         if gene_row:
#         #             gene_ids.append(gene_row["id"])
#         #             gene_symbols[gene_row["id"]] = {
#         #                 "symbol": gene_row["gene_symbol"],
#         #                 "ensembl_id": gene_row["ensembl_id"]
#         #             }
#         #             selected_gene_set.add(gene_row["gene_symbol"])
#         #         else:
#         #             debug_info[gene_input] = {"error": f"Gene '{gene_input}' not found."}
#         # else:
#         selected_gene_set = set()
#         gene_symbols = {}
#         gene_ids = []
#         if genes:
#             # Split comma-separated genes from frontend
#             all_gene_inputs = []
#             for gene_input in genes:
#                 split_genes = [g.strip().upper() for g in str(gene_input).split(",") if g.strip()]
#                 all_gene_inputs.extend(split_genes)
            
#             # Remove duplicates, preserve order
#             unique_gene_inputs = []
#             seen = set()
#             for gene in all_gene_inputs:
#                 if gene not in seen:
#                     unique_gene_inputs.append(gene)
#                     seen.add(gene)
            
#             # Lookup each individual gene
#             for gene_input in unique_gene_inputs:
#                 cur.execute(
#                     "SELECT id, gene_symbol, ensembl_id FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
#                     (gene_input, gene_input),
#                 )
#                 gene_row = cur.fetchone()
#                 if gene_row:
#                     gene_ids.append(gene_row["id"])
#                     gene_symbols[gene_row["id"]] = {
#                         "symbol": gene_row["gene_symbol"],
#                         "ensembl_id": gene_row["ensembl_id"]
#                     }
#                     selected_gene_set.add(gene_row["gene_symbol"])
#                 else:
#                     debug_info[gene_input] = {"error": f"Gene '{gene_input}' not found."}
#         else:
#             # Fallback: Select top genes by logFC
#             warning = "No genes provided. Selecting top differentially expressed genes by logFC."
#             for site in cancer_sites:
#                 cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#                 site_row = cur.fetchone()
#                 if not site_row:
#                     continue
#                 site_id = site_row["id"]
#                 if cancer_types:
#                     cur.execute(
#                         "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                         (site_id, tuple(cancer_types)),
#                     )
#                 else:
#                     cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#                 cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]
#                 if not cancer_types_list:
#                     continue
#                 cur.execute("SELECT id, gene_symbol FROM genes LIMIT 100")  # Limit for performance
#                 all_genes = cur.fetchall()
#                 gene_logfc = []
#                 for gene in all_genes:
#                     for ct in cancer_types_list:
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.tpm AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene["id"]),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             continue
#                         df = pd.DataFrame(rows)
#                         tumor = pd.Series(df[df["sample_type"].str.lower() == "tumor"]["expr"])
#                         normal = pd.Series(df[df["sample_type"].str.lower() == "normal"]["expr"])
#                         tumor_stats = compute_metrics(tumor)
#                         normal_stats = compute_metrics(normal)
#                         if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                             logfc = math.log2(tumor_stats["cv"] / normal_stats["cv"])
#                             if not np.isnan(logfc) and abs(logfc) >= logfc_threshold:
#                                 gene_logfc.append((gene["id"], gene["gene_symbol"], abs(logfc)))
#                 # Select top 20 genes by |logFC|, or all if fewer
#                 gene_logfc.sort(key=lambda x: x[2], reverse=True)
#                 top_genes = gene_logfc[:20]
#                 if not top_genes:
#                     warning = f"{warning} No genes met logFC threshold for {site}."
#                 for gene_id, gene_symbol, _ in top_genes:
#                     gene_ids.append(gene_id)
#                     gene_symbols[gene_id] = {"symbol": gene_symbol, "ensembl_id": gene_symbol}
#                     selected_gene_set.add(gene_symbol)

#         # Process sample counts and gene stats
#         print(cancer_sites)
#         for site in cancer_sites:
#             print(site)
#             cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#             site_row = cur.fetchone()
#             if not site_row:
#                 debug_info[site] = {"error": f"Site '{site}' not found."}
#                 continue
#             site_id = site_row["id"]

#             if cancer_types:
#                 cur.execute(
#                     "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                     (site_id, tuple(cancer_types)),
#                 )
#             else:
#                 cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#             cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]

#             if not cancer_types_list:
#                 debug_info[site] = {"error": f"No cancer types found for site '{site}'."}
#                 continue

#             cur.execute(
#                 """
#                 SELECT s.sample_type, COUNT(*) AS count
#                 FROM samples s
#                 JOIN cancer_types c ON c.id = s.cancer_type_id
#                 WHERE c.site_id = %s AND c.tcga_code IN %s
#                 GROUP BY s.sample_type
#                 """,
#                 (site_id, tuple(cancer_types_list)),
#             )
#             site_counts = {"tumor": 0, "normal": 0}
#             for row in cur.fetchall():
#                 stype = row["sample_type"].lower()
#                 if stype in site_counts:
#                     site_counts[stype] = row["count"]
#             sample_counts[site] = site_counts

#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 gene_stats = {}
#                 heatmap_data = {}
#                 noise_metrics = {}
#                 gene_stats_log = {}
#                 heatmap_data_log = {}
#                 noise_metrics_log = {}

#                 for gene_id in gene_ids:
#                     all_tumor_vals, all_normal_vals = [], []

#                     for ct in cancer_types_list:
#                         print(ct)
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.{norm_method} AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene_id),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             debug_info.setdefault(site, {}).setdefault("no_data", []).append(
#                                 f"No expression data for gene {gene_symbols.get(gene_id, {}).get('symbol', gene_id)} in {norm_method}"
#                             )
#                             continue
#                         df = pd.DataFrame(rows)
#                         all_tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"].tolist())
#                         all_normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"].tolist())

#                     tumor = pd.Series(all_tumor_vals)
#                     normal = pd.Series(all_normal_vals)

#                     tumor_stats = compute_metrics(tumor)
#                     normal_stats = compute_metrics(normal)
#                     logfc_raw = np.nan
#                     if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                         logfc_raw = math.log2(tumor_stats["cv"] / normal_stats["cv"])

#                     df_all = pd.DataFrame({
#                         "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
#                         "expr": list(tumor) + list(normal),
#                     })
#                     df_all["log2_expr"] = np.log2(df_all["expr"] + 1)

#                     tumor_log = df_all[df_all["sample_type"] == "tumor"]["log2_expr"]
#                     normal_log = df_all[df_all["sample_type"] == "normal"]["log2_expr"]

#                     tumor_stats_log = compute_metrics(tumor_log)
#                     normal_stats_log = compute_metrics(normal_log)
#                     logfc_log = np.nan
#                     if tumor_stats_log and normal_stats_log and normal_stats_log["cv"] > 0:
#                         logfc_log = math.log2(tumor_stats_log["cv"] / normal_stats_log["cv"])

#                     gene_symbol = gene_symbols.get(gene_id, {"symbol": f"gene_{gene_id}"}).get("symbol")
#                     ensembl_id = gene_symbols.get(gene_id, {"ensembl_id": ""}).get("ensembl_id")

#                     gene_stats[gene_symbol] = {
#                         **gene_stats.get(gene_symbol, {}),
#                         site.lower(): {
#                             "mean_tumor": tumor_stats["mean"] if tumor_stats else None,
#                             "mean_normal": normal_stats["mean"] if normal_stats else None,
#                             "cv_tumor": tumor_stats["cv"] if tumor_stats else None,
#                             "cv_normal": normal_stats["cv"] if normal_stats else None,
#                             "logfc": logfc_raw,
#                             "ensembl_id": ensembl_id
#                         }
#                     }
#                     heatmap_data[gene_symbol] = {
#                         **heatmap_data.get(gene_symbol, {}),
#                         f"{site}_normal": normal_stats["mean"] if normal_stats else 0,
#                         f"{site}_tumor": tumor_stats["mean"] if tumor_stats else 0
#                     }
#                     noise_metrics[gene_symbol] = {
#                         **noise_metrics.get(gene_symbol, {}),
#                         site.lower(): {
#                             "cv_normal": normal_stats["cv"] if normal_stats else 0,
#                             "cv_tumor": tumor_stats["cv"] if tumor_stats else 0,
#                             "logfc": logfc_raw
#                         }
#                     }

#                     gene_stats_log[gene_symbol] = {
#                         **gene_stats_log.get(gene_symbol, {}),
#                         site.lower(): {
#                             "mean_tumor": tumor_stats_log["mean"] if tumor_stats_log else None,
#                             "mean_normal": normal_stats_log["mean"] if normal_stats_log else None,
#                             "cv_tumor": tumor_stats_log["cv"] if tumor_stats_log else None,
#                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else None,
#                             "logfc": logfc_log,
#                             "ensembl_id": ensembl_id
#                         }
#                     }
#                     heatmap_data_log[gene_symbol] = {
#                         **heatmap_data_log.get(gene_symbol, {}),
#                         f"{site}_normal": normal_stats_log["mean"] if normal_stats_log else 0,
#                         f"{site}_tumor": tumor_stats_log["mean"] if normal_stats_log else 0
#                     }
#                     noise_metrics_log[gene_symbol] = {
#                         **noise_metrics_log.get(gene_symbol, {}),
#                         site.lower(): {
#                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else 0,
#                             "cv_tumor": tumor_stats_log["cv"] if normal_stats_log else 0,
#                             "logfc": logfc_log
#                         }
#                     }

#                 results["raw"][norm_method]["gene_stats"].update(gene_stats)
#                 results["raw"][norm_method]["heatmap_data"].update(heatmap_data)
#                 results["raw"][norm_method]["noise_metrics"].update(noise_metrics)
#                 results["log2"][norm_method]["gene_stats"].update(gene_stats_log)
#                 results["log2"][norm_method]["heatmap_data"].update(heatmap_data_log)
#                 results["log2"][norm_method]["noise_metrics"].update(noise_metrics_log)

#         enrichment_results = []
#         if selected_gene_set:
#             try:
#                 enr = gp.enrichr(
#                     gene_list=list(selected_gene_set),
#                     gene_sets=library,
#                     organism='human',
#                     outdir=None
#                 )
#                 res = enr.results
#                 if res.empty:
#                     warning = f"No significant pathways found for genes: {list(selected_gene_set)} in library: {library}"
#                 else:
#                     res = res[res["Adjusted P-value"] <= 0.2]
#                     if res.empty:
#                         warning = f"No pathways with Adjusted P-value <= 0.2 for genes: {list(selected_gene_set)} in library: {library}"
#                     res.sort_values("Adjusted P-value", inplace=True)
#                     res = res.head(top_n)
#                     for _, row in res.iterrows():
#                         enrichment_results.append({
#                             "Term": row["Term"],
#                             "P-value": row["P-value"],
#                             "Adjusted P-value": row["Adjusted P-value"],
#                             "Combined Score": row["Combined Score"],
#                             "Odds Ratio": row["Odds Ratio"],
#                             "Genes": row["Genes"].split(";"),
#                             "GeneSet": library,
#                             "Overlap": row["Overlap"],
#                         })
#             except Exception as e:
#                 warning = f"Enrichr analysis failed: {str(e)}"

#         # ✅ CORRECT: Assign SINGLE enrichment to ALL norm/scale combinations
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method]["enrichment"] = enrichment_results
#                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)

#         # Perform enrichment if ORA and genes available
#         # enrichment_results = []
#         # if analysis_type == "ORA" and selected_gene_set:
#         #     try:
#         #         enr = gp.enrichr(
#         #             gene_list=list(selected_gene_set),
#         #             gene_sets=library,
#         #             organism='human',
#         #             outdir=None
#         #         )
#         #         res = enr.results
#         #         if res.empty:
#         #             warning = f"No significant pathways found for genes: {list(selected_gene_set)} in library: {library}"
#         #         else:
#         #             # Relaxed p-value filter
#         #             res = res[res["Adjusted P-value"] <= 0.2]
#         #             if res.empty:
#         #                 warning = f"No pathways with Adjusted P-value <= 0.2 for genes: {list(selected_gene_set)} in library: {library}"
#         #             res.sort_values("Adjusted P-value", inplace=True)
#         #             res = res.head(top_n)
#         #             for _, row in res.iterrows():
#         #                 enrichment_results.append({
#         #                     "Term": row["Term"],
#         #                     "P-value": row["P-value"],
#         #                     "Adjusted P-value": row["Adjusted P-value"],
#         #                     "Combined Score": row["Combined Score"],
#         #                     "Odds Ratio": row["Odds Ratio"],
#         #                     "Genes": row["Genes"].split(";"),
#         #                     "GeneSet": library,
#         #                     "Overlap": row["Overlap"],
#         #                 })
#         #     except Exception as e:
#         #         warning = f"Enrichr analysis failed for genes {list(selected_gene_set)} in library {library}: {str(e)}"
#         # elif analysis_type == "GSEA":
#         #     warning = "GSEA not implemented yet. No enrichment performed."
#         # elif not selected_gene_set:
#         #     warning = "No valid genes found for enrichment analysis."

#         # # Assign enrichment to all norm and scales
#         # for scale in ["raw", "log2"]:
#         #     for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#         #         results[scale][norm_method]["enrichment"] = enrichment_results
#         #         results[scale][norm_method]["top_genes"] = list(selected_gene_set)
        
#         final_output = {
#             "raw": results["raw"],
#             "log2": results["log2"],
#             "sample_counts": sample_counts,
#             "available_sites": available_sites,
#             "warning": warning,
#             "debug": debug_info if debug_info else None,
#         }
#         print(final_output)

#         return JSONResponse(sanitize_floats(final_output))

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
#     finally:
#         conn.close()
# @app.get("/api/pathway-analysis")
# def get_pathway_analysis(
#     cancer: str = Query(..., description="Comma-separated cancer sites or single site (e.g., Lung,Liver or Lung)"),
#     cancer_types: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
#     genes: list[str] = Query(None, description="Optional list of gene IDs or symbols for ORA (e.g., TP53,EGFR)"),
#     top_n: int = Query(15, description="Number of top pathways to return"),
#     analysis_type: str = Query("ORA", description="Type of analysis: ORA or GSEA"),
#     site_analysis_type: str = Query("pan-cancer", description="Analysis scope: pan-cancer or cancer-specific"),
#     logfc_threshold: float = Query(0.7, description="LogFC threshold for selecting significant genes")
# ):
#     """
#     Perform pathway enrichment analysis for specified cancer sites and genes using Enrichr.
#     Returns JSON with structure:
#     {
#       "raw": {
#         norm_method: {
#           "enrichment": [{Term, P-value, Adjusted P-value, Combined Score, Odds Ratio, Genes, GeneSet}],
#           "top_genes": [str],
#           "gene_stats": {gene: {site: {mean_tumor, mean_normal, cv_tumor, cv_normal, logfc}}},
#           "heatmap_data": {gene: {site_normal: float, site_tumor: float}},
#           "noise_metrics": {gene: {site: {cv_tumor, cv_normal, logfc}}}
#         }
#       },
#       "log2": {...},
#       "sample_counts": {site: {tumor: int, normal: int}},
#       "available_sites": [{id: int, name: str}],
#       "warning": str,
#       "debug": dict
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)

#     cancer_sites = [s.strip() for s in cancer.split(",")]
#     results = {"raw": {}, "log2": {}}
#     sample_counts = {}
#     debug_info = {}
#     available_sites = []
#     warning = None

#     try:
#         # Fetch available sites
#         cur.execute("SELECT id, name FROM sites")
#         available_sites = cur.fetchall()

#         # Validate analysis type
#         if analysis_type not in ["ORA", "GSEA"]:
#             raise HTTPException(status_code=400, detail="Invalid analysis_type. Must be 'ORA' or 'GSEA'.")
#         if site_analysis_type not in ["pan-cancer", "cancer-specific"]:
#             raise HTTPException(status_code=400, detail="Invalid site_analysis_type. Must be 'pan-cancer' or 'cancer-specific'.")

#         # Initialize results structures
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method] = {
#                     "enrichment": [],
#                     "top_genes": [],
#                     "gene_stats": {},
#                     "heatmap_data": {},
#                     "noise_metrics": {},
#                 }

#         # Fetch gene info
#         selected_gene_set = set()
#         gene_symbols = {}
#         gene_ids = []
#         if genes:
#             for gene_input in genes:
#                 cur.execute(
#                     "SELECT id, gene_symbol, ensembl_id FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
#                     (gene_input, gene_input),
#                 )
#                 gene_row = cur.fetchone()
#                 if gene_row:
#                     gene_ids.append(gene_row["id"])
#                     gene_symbols[gene_row["id"]] = {
#                         "symbol": gene_row["gene_symbol"],
#                         "ensembl_id": gene_row["ensembl_id"]
#                     }
#                     selected_gene_set.add(gene_row["gene_symbol"])
#                 else:
#                     debug_info[gene_input] = {"error": f"Gene '{gene_input}' not found."}
#         else:
#             # Fallback: Select top genes by logFC
#             warning = "No genes provided. Selecting top differentially expressed genes by logFC."
#             for site in cancer_sites:
#                 cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#                 site_row = cur.fetchone()
#                 if not site_row:
#                     continue
#                 site_id = site_row["id"]
#                 if cancer_types:
#                     cur.execute(
#                         "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                         (site_id, tuple(cancer_types)),
#                     )
#                 else:
#                     cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#                 cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]
#                 if not cancer_types_list:
#                     continue
#                 cur.execute("SELECT id, gene_symbol FROM genes LIMIT 100")  # Limit for performance
#                 all_genes = cur.fetchall()
#                 gene_logfc = []
#                 for gene in all_genes:
#                     for ct in cancer_types_list:
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.tpm AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene["id"]),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             continue
#                         df = pd.DataFrame(rows)
#                         tumor = pd.Series(df[df["sample_type"].str.lower() == "tumor"]["expr"])
#                         normal = pd.Series(df[df["sample_type"].str.lower() == "normal"]["expr"])
#                         tumor_stats = compute_metrics(tumor)
#                         normal_stats = compute_metrics(normal)
#                         if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                             logfc = math.log2(tumor_stats["cv"] / normal_stats["cv"])
#                             if not np.isnan(logfc) and abs(logfc) >= logfc_threshold:
#                                 gene_logfc.append((gene["id"], gene["gene_symbol"], abs(logfc)))
#                 # Select top 20 genes by |logFC|, or all if fewer
#                 gene_logfc.sort(key=lambda x: x[2], reverse=True)
#                 top_genes = gene_logfc[:20]
#                 if not top_genes:
#                     warning = f"{warning} No genes met logFC threshold for {site}."
#                 for gene_id, gene_symbol, _ in top_genes:
#                     gene_ids.append(gene_id)
#                     gene_symbols[gene_id] = {"symbol": gene_symbol, "ensembl_id": gene_symbol}
#                     selected_gene_set.add(gene_symbol)

#         # Process sample counts and gene stats
#         for site in cancer_sites:
#             cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#             site_row = cur.fetchone()
#             if not site_row:
#                 debug_info[site] = {"error": f"Site '{site}' not found."}
#                 continue
#             site_id = site_row["id"]

#             if cancer_types:
#                 cur.execute(
#                     "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                     (site_id, tuple(cancer_types)),
#                 )
#             else:
#                 cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#             cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]

#             if not cancer_types_list:
#                 debug_info[site] = {"error": f"No cancer types found for site '{site}'."}
#                 continue

#             cur.execute(
#                 """
#                 SELECT s.sample_type, COUNT(*) AS count
#                 FROM samples s
#                 JOIN cancer_types c ON c.id = s.cancer_type_id
#                 WHERE c.site_id = %s AND c.tcga_code IN %s
#                 GROUP BY s.sample_type
#                 """,
#                 (site_id, tuple(cancer_types_list)),
#             )
#             site_counts = {"tumor": 0, "normal": 0}
#             for row in cur.fetchall():
#                 stype = row["sample_type"].lower()
#                 if stype in site_counts:
#                     site_counts[stype] = row["count"]
#             sample_counts[site] = site_counts

#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 gene_stats = {}
#                 heatmap_data = {}
#                 noise_metrics = {}
#                 gene_stats_log = {}
#                 heatmap_data_log = {}
#                 noise_metrics_log = {}

#                 for gene_id in gene_ids:
#                     all_tumor_vals, all_normal_vals = [], []

#                     for ct in cancer_types_list:
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.{norm_method} AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene_id),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             debug_info.setdefault(site, {}).setdefault("no_data", []).append(
#                                 f"No expression data for gene {gene_symbols.get(gene_id, {}).get('symbol', gene_id)} in {norm_method}"
#                             )
#                             continue
#                         df = pd.DataFrame(rows)
#                         all_tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"].tolist())
#                         all_normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"].tolist())

#                     tumor = pd.Series(all_tumor_vals)
#                     normal = pd.Series(all_normal_vals)

#                     tumor_stats = compute_metrics(tumor)
#                     normal_stats = compute_metrics(normal)
#                     logfc_raw = np.nan
#                     if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                         logfc_raw = math.log2(tumor_stats["cv"] / normal_stats["cv"])

#                     df_all = pd.DataFrame({
#                         "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
#                         "expr": list(tumor) + list(normal),
#                     })
#                     df_all["log2_expr"] = np.log2(df_all["expr"] + 1)

#                     tumor_log = df_all[df_all["sample_type"] == "tumor"]["log2_expr"]
#                     normal_log = df_all[df_all["sample_type"] == "normal"]["log2_expr"]

#                     tumor_stats_log = compute_metrics(tumor_log)
#                     normal_stats_log = compute_metrics(normal_log)
#                     logfc_log = np.nan
#                     if tumor_stats_log and normal_stats_log and normal_stats_log["cv"] > 0:
#                         logfc_log = math.log2(tumor_stats_log["cv"] / normal_stats_log["cv"])

#                     gene_symbol = gene_symbols.get(gene_id, {"symbol": f"gene_{gene_id}"}).get("symbol")
#                     ensembl_id = gene_symbols.get(gene_id, {"ensembl_id": ""}).get("ensembl_id")

#                     gene_stats[gene_symbol] = {
#                         **gene_stats.get(gene_symbol, {}),
#                         site.lower(): {
#                             "mean_tumor": tumor_stats["mean"] if tumor_stats else None,
#                             "mean_normal": normal_stats["mean"] if normal_stats else None,
#                             "cv_tumor": tumor_stats["cv"] if tumor_stats else None,
#                             "cv_normal": normal_stats["cv"] if normal_stats else None,
#                             "logfc": logfc_raw,
#                             "ensembl_id": ensembl_id
#                         }
#                     }
#                     heatmap_data[gene_symbol] = {
#                         **heatmap_data.get(gene_symbol, {}),
#                         f"{site}_normal": normal_stats["mean"] if normal_stats else 0,
#                         f"{site}_tumor": tumor_stats["mean"] if tumor_stats else 0
#                     }
#                     noise_metrics[gene_symbol] = {
#                         **noise_metrics.get(gene_symbol, {}),
#                         site.lower(): {
#                             "cv_normal": normal_stats["cv"] if normal_stats else 0,
#                             "cv_tumor": tumor_stats["cv"] if tumor_stats else 0,
#                             "logfc": logfc_raw
#                         }
#                     }

#                     gene_stats_log[gene_symbol] = {
#                         **gene_stats_log.get(gene_symbol, {}),
#                         site.lower(): {
#                             "mean_tumor": tumor_stats_log["mean"] if tumor_stats_log else None,
#                             "mean_normal": normal_stats_log["mean"] if normal_stats_log else None,
#                             "cv_tumor": tumor_stats_log["cv"] if tumor_stats_log else None,
#                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else None,
#                             "logfc": logfc_log,
#                             "ensembl_id": ensembl_id
#                         }
#                     }
#                     heatmap_data_log[gene_symbol] = {
#                         **heatmap_data_log.get(gene_symbol, {}),
#                         f"{site}_normal": normal_stats_log["mean"] if normal_stats_log else 0,
#                         f"{site}_tumor": tumor_stats_log["mean"] if tumor_stats_log else 0
#                     }
#                     noise_metrics_log[gene_symbol] = {
#                         **noise_metrics_log.get(gene_symbol, {}),
#                         site.lower(): {
#                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else 0,
#                             "cv_tumor": normal_stats_log["cv"] if normal_stats_log else 0,
#                             "logfc": logfc_log
#                         }
#                     }

#                 results["raw"][norm_method]["gene_stats"].update(gene_stats)
#                 results["raw"][norm_method]["heatmap_data"].update(heatmap_data)
#                 results["raw"][norm_method]["noise_metrics"].update(noise_metrics)
#                 results["log2"][norm_method]["gene_stats"].update(gene_stats_log)
#                 results["log2"][norm_method]["heatmap_data"].update(heatmap_data_log)
#                 results["log2"][norm_method]["noise_metrics"].update(noise_metrics_log)

#         # Perform enrichment if ORA and genes available
#         enrichment_results = []
#         if analysis_type == "ORA" and selected_gene_set:
#             libraries = ['KEGG_2021_Human', 'Reactome_2022', 'GO_Biological_Process_2023', 'GO_Molecular_Function_2023', 'GO_Cellular_Component_2023']
#             try:
#                 enr = gp.enrichr(gene_list=list(selected_gene_set),
#                                 gene_sets=libraries,
#                                 organism='human',
#                                 outdir=None)
#                 res = enr.results
#                 if res.empty:
#                     warning = f"No significant pathways found for genes: {list(selected_gene_set)}"
#                 else:
#                     # Relaxed p-value filter
#                     res = res[res["Adjusted P-value"] <= 0.2]
#                     if res.empty:
#                         warning = f"No pathways with Adjusted P-value <= 0.2 for genes: {list(selected_gene_set)}"
#                     res.sort_values("Adjusted P-value", inplace=True)
#                     res = res.head(top_n)
#                     for _, row in res.iterrows():
#                         enrichment_results.append({
#                             "Term": row["Term"],
#                             "P-value": row["P-value"],
#                             "Adjusted P-value": row["Adjusted P-value"],
#                             "Combined Score": row["Combined Score"],
#                             "Odds Ratio": row["Odds Ratio"],
#                             "Genes": row["Genes"].split(";"),
#                             "GeneSet": row["Gene_set"],
#                             "Overlap": row["Overlap"],
#                         })
#             except Exception as e:
#                 warning = f"Enrichr analysis failed for genes {list(selected_gene_set)}: {str(e)}"
#         elif analysis_type == "GSEA":
#             warning = "GSEA not implemented yet. No enrichment performed."
#         elif not selected_gene_set:
#             warning = "No valid genes found for enrichment analysis."

#         # Assign enrichment to all norm and scales
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method]["enrichment"] = enrichment_results
#                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)

#         final_output = {
#             "raw": results["raw"],
#             "log2": results["log2"],
#             "sample_counts": sample_counts,
#             "available_sites": available_sites,
#             "warning": warning,
#             "debug": debug_info if debug_info else None,
#         }
#         print(final_output)

#         return JSONResponse(sanitize_floats(final_output))

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
#     finally:
#         conn.close()
# @app.get("/api/pathway-analysis")
# def get_pathway_analysis(
#     cancer: str = Query(..., description="Comma-separated cancer sites (e.g., Lung,Liver)"),
#     genes: List[str] = Query(None, description="Optional gene symbols (e.g., TP53,EGFR)"),
#     top_n: int = Query(50, description="Number of top pathways or neighbors"),
#     logfc_threshold: float = Query(0.7, description="LogFC threshold"),
#     mode: str = Query("enrichment", description="STRING mode: enrichment or neighbors ('enrichment'|'neighbors')"),
#     network_type: str = Query("functional", description="Network type for neighbors: functional or physical"),
#     score_threshold: float = Query(0.4, description="STRING confidence threshold (0–1)"),
# ):
#     conn = get_connection()
#     cur = conn.cursor()
    
#     try:
#         # Parse inputs
#         cancer_sites = [s.strip() for s in cancer.split(",") if s.strip()]
        
#         # Get genes
#         gene_list, selected_gene_set, warning = get_genes(cur, genes, cancer_sites, logfc_threshold)
        
#         # Get sample counts
#         sample_counts = get_sample_counts(cur, cancer_sites)
        
#         # Compute gene stats
#         results = compute_gene_stats(cur, cancer_sites, gene_list)

#         # Run STRING logic
#         if mode == "neighbors":
#             string_results = run_stringdb_neighbors(selected_gene_set, network_type, score_threshold, top_n)
#             for scale in ["raw", "log2"]:
#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     results[scale][norm_method]["network"] = string_results
#         else:
#             string_results = run_stringdb_enrichment(selected_gene_set, top_n)
#             for scale in ["raw", "log2"]:
#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     results[scale][norm_method]["enrichment"] = string_results

#         # Assign top genes
#         for scale in ["raw", "log2"]:
#             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)
        
#         # Build response
#         output = {
#             "raw": results["raw"],
#             "log2": results["log2"],
#             "sample_counts": sample_counts,
#             "available_sites": get_available_sites(cur),
#             "warning": warning,
#             "mode": mode,
#         }
#         return sanitize_floats(output)
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         print(f"Error: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()
# # def sanitize_floats(obj):
# #     """Recursively replace NaN, inf, and -inf with None for JSON serialization."""
# #     if isinstance(obj, dict):
# #         return {k: sanitize_floats(v) for k, v in obj.items()}
# #     elif isinstance(obj, list):
# #         return [sanitize_floats(v) for v in obj]
# #     elif isinstance(obj, float):
# #         if math.isnan(obj) or math.isinf(obj):
# #             return None
# #         return obj
# #     return obj

# # def get_connection():
# #     return pymysql.connect(**DB_CONFIG)

# # def compute_metrics(values: pd.Series):
# #     """Compute mean, std, mad, cv, and cv² for a numeric series."""
# #     if len(values) == 0:
# #         return None
# #     mean = mean_calculation(values)
# #     std = std_calculation(values)
# #     mad = mad_calculation(values)
# #     cv = (std / mean) * 100 if mean != 0 else np.nan
# #     cv2 = cv**2 if not np.isnan(cv) else np.nan
# #     return {"mean": mean, "std": std, "mad": mad, "cv": cv, "cv_squared": cv2}

# # def perform_ora(genes: List[str], pathway_genes: Dict[str, List[str]], background_genes: List[str]):
# #     """Perform Over-Representation Analysis (ORA) for given genes."""
# #     results = []
# #     for term, pathway_gene_list in pathway_genes.items():
# #         # Intersection of input genes and pathway genes
# #         overlap = set(genes) & set(pathway_gene_list)
# #         if not overlap:
# #             continue

# #         # Contingency table for hypergeometric test
# #         k = len(overlap)  # Number of successes (genes in pathway)
# #         M = len(background_genes)  # Total population
# #         n = len(pathway_gene_list)  # Number of successes in population
# #         N = len(genes)  # Number of draws

# #         # Hypergeometric test
# #         p_value = stats.hypergeom.sf(k - 1, M, n, N)
# #         odds_ratio, _ = stats.fisher_exact([[k, n - k], [N - k, M - n - (N - k)]], alternative='greater')

# #         results.append({
# #             "Term": term,
# #             "P-value": p_value,
# #             "Genes": list(overlap),
# #             "Overlap": f"{len(overlap)}/{len(pathway_gene_list)}",
# #             "Odds Ratio": odds_ratio,
# #             "GeneSet": term,
# #         })

# #     # Adjust p-values
# #     if results:
# #         p_values = [r["P-value"] for r in results]
# #         adjusted_p = multipletests(p_values, method='fdr_bh')[1]
# #         for i, r in enumerate(results):
# #             r["Adjusted P-value"] = adjusted_p[i]
# #             r["Combined Score"] = -math.log10(r["Adjusted P-value"]) * r["Odds Ratio"] if r["Adjusted P-value"] > 0 else 0

# #     return sorted(results, key=lambda x: x["Adjusted P-value"])

# # @app.get("/api/genes")
# # def get_genes():
# #     """Fetch all available gene symbols."""
# #     conn = get_connection()
# #     cur = conn.cursor(pymysql.cursors.DictCursor)
# #     try:
# #         cur.execute("SELECT DISTINCT gene_symbol FROM genes WHERE gene_symbol IS NOT NULL ORDER BY gene_symbol ASC")
# #         rows = cur.fetchall()
# #         genes = [row["gene_symbol"] for row in rows]
# #         return {"genes": genes}
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
# #     finally:
# #         conn.close()

# # @app.get("/api/sites")
# # def get_sites():
# #     """Fetch all available cancer sites."""
# #     conn = get_connection()
# #     cur = conn.cursor(pymysql.cursors.DictCursor)
# #     try:
# #         cur.execute("SELECT id, name FROM sites ORDER BY name ASC")
# #         sites = cur.fetchall()
# #         return JSONResponse({"sites": sites})
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
# #     finally:
# #         conn.close()


# # @app.get("/api/cancer_types")
# # def get_cancer_types(site_ids: List[int] = Query(..., description="List of site IDs")):
# #     """Fetch cancer types for given site IDs."""
# #     conn = get_connection()
# #     cur = conn.cursor(pymysql.cursors.DictCursor)
# #     try:
# #         cur.execute(
# #             "SELECT tcga_code, site_id FROM cancer_types WHERE site_id IN %s",
# #             (tuple(site_ids),),
# #         )
# #         cancer_types = cur.fetchall()
# #         return JSONResponse({"cancer_types": cancer_types})
# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
# #     finally:
# #         conn.close()

# @app.get("/api/gene_noise")
# def get_gene_noise(
#     cancer_site: list[str] = Query(..., description="One or more cancer sites (e.g., Lung, Liver, Adrenal Gland)"),
#     cancer_type: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
#     gene_ids: list[str] = Query(..., description="One or more gene IDs or symbols (e.g., TP53, EGFR)")
# ):
#     """
#     Compute expression noise metrics for one or more cancer sites and genes.
#     Returns JSON with structure:
#     {
#       site: {
#         gene_symbol: {
#           norm_method: { "raw": {...}, "log2": {...} }
#         }
#       }
#     }
#     """
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)

#     results = {}
#     debug_info = {}
#     sample_counts = {}

#     try:
#         for site in cancer_site:
#             cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
#             site_row = cur.fetchone()
#             if not site_row:
#                 debug_info[site] = {"error": f"Site '{site}' not found."}
#                 continue
#             site_id = site_row["id"]

#             # Determine all cancer types for this site
#             if cancer_type:
#                 cur.execute(
#                     "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
#                     (site_id, tuple(cancer_type)),
#                 )
#             else:
#                 cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
#             cancer_types = [row["tcga_code"] for row in cur.fetchall()]

#             if not cancer_types:
#                 debug_info[site] = {"error": f"No cancer types found for site '{site}'."}
#                 continue

#             # Count tumor/normal samples for the site
#             cur.execute(
#                 """
#                 SELECT s.sample_type, COUNT(*) AS count
#                 FROM samples s
#                 JOIN cancer_types c ON c.id = s.cancer_type_id
#                 WHERE c.site_id = %s AND c.tcga_code IN %s
#                 GROUP BY s.sample_type
#                 """,
#                 (site_id, tuple(cancer_types)),
#             )
#             site_counts = {"tumor": 0, "normal": 0}
#             for row in cur.fetchall():
#                 stype = row["sample_type"].lower()
#                 if stype in site_counts:
#                     site_counts[stype] = row["count"]
#             sample_counts[site] = site_counts

#             # Initialize results for this site
#             results[site] = {}

#             # Loop over all requested genes
#             for gene_input in gene_ids:
#                 cur.execute(
#                     "SELECT id, ensembl_id, gene_symbol FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
#                     (gene_input, gene_input),
#                 )
#                 gene_row = cur.fetchone()
#                 if not gene_row:
#                     debug_info.setdefault(site, {})[gene_input] = {"error": f"Gene '{gene_input}' not found."}
#                     continue

#                 gene_id = gene_row["id"]
#                 gene_symbol = gene_row["gene_symbol"]
#                 ensembl_id = gene_row["ensembl_id"]

#                 # Prepare gene entry
#                 results[site][gene_symbol] = {}

#                 for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#                     all_tumor_vals, all_normal_vals = [], []

#                     for ct in cancer_types:
#                         cur.execute(
#                             f"""
#                             SELECT s.sample_type, e.{norm_method} AS expr
#                             FROM gene_expressions e
#                             JOIN samples s ON s.id = e.sample_id
#                             JOIN cancer_types c ON c.id = s.cancer_type_id
#                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
#                             """,
#                             (site_id, ct, gene_id),
#                         )
#                         rows = cur.fetchall()
#                         if not rows:
#                             continue
#                         df = pd.DataFrame(rows)
#                         all_tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"].tolist())
#                         all_normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"].tolist())

#                     tumor = pd.Series(all_tumor_vals)
#                     normal = pd.Series(all_normal_vals)

#                     tumor_stats = compute_metrics(tumor)
#                     normal_stats = compute_metrics(normal)
#                     logfc_raw = np.nan
#                     if tumor_stats and normal_stats and normal_stats["cv"] > 0:
#                         logfc_raw = math.log2(tumor_stats["cv"] / normal_stats["cv"])

                    

#                     # Log2-transformed
#                     df_all = pd.DataFrame({
#                         "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
#                         "expr": list(tumor) + list(normal),
#                     })
#                     df_all["log2_expr"] = np.log2(df_all["expr"] + 1)

#                     tumor_log = df_all[df_all["sample_type"] == "tumor"]["log2_expr"]
#                     normal_log = df_all[df_all["sample_type"] == "normal"]["log2_expr"]

#                     tumor_stats_log = compute_metrics(tumor_log)
#                     normal_stats_log = compute_metrics(normal_log)
#                     logfc_log = np.nan
#                     if tumor_stats_log and normal_stats_log:
#                         logfc_log = tumor_stats_log["cv"] - normal_stats_log["cv"]

#                     results[site][gene_symbol][norm_method] = {
#                         "raw": {
#                             **(tumor_stats or {}),
#                             **{f"{k}_normal": v for k, v in (normal_stats or {}).items()},
#                             "logfc": logfc_log,
#                         },
#                         "log2": {
#                             **(tumor_stats_log or {}),
#                             **{f"{k}_normal": v for k, v in (normal_stats_log or {}).items()},
#                             "logfc": logfc_log,
#                         },
                        
#                     }
#                     print(logfc_raw, logfc_log)

#         final_output = {
#             "results": sanitize_floats(results),
#             "sample_counts": sample_counts,
#         }

#     except Exception as e:
#         final_output = {"error": f"Unexpected error: {str(e)}"}
#     finally:
#         conn.close()

#     # Clean up NaN, inf, etc. for JSON compliance
#     results = sanitize_floats(final_output)
#     print(results)
#     return JSONResponse(results)
#     # return JSONResponse(final_output)

# # @app.get("/api/pathway-analysis")
# # def get_pathway_analysis(
# #     cancer: str = Query(..., description="Comma-separated cancer sites or single site (e.g., Lung,Liver or Lung)"),
# #     cancer_types: list[str] = Query(None, description="Optional list of cancer types (TCGA codes)"),
# #     genes: list[str] = Query(None, description="Optional list of gene IDs or symbols for ORA (e.g., TP53,EGFR)"),
# #     top_n: int = Query(15, description="Number of top pathways to return"),
# #     analysis_type: str = Query("ORA", description="Type of analysis: ORA or GSEA"),
# #     site_analysis_type: str = Query("pan-cancer", description="Analysis scope: pan-cancer or cancer-specific"),
# #     logfc_threshold: float = Query(0.7, description="LogFC threshold for selecting significant genes")
# # ):
# #     """
# #     Perform pathway enrichment analysis for specified cancer sites and genes using Enrichr.
# #     Returns JSON with structure:
# #     {
# #       "raw": {
# #         norm_method: {
# #           "enrichment": [{Term, P-value, Adjusted P-value, Combined Score, Odds Ratio, Genes, GeneSet}],
# #           "top_genes": [str],
# #           "gene_stats": {gene: {site: {mean_tumor, mean_normal, cv_tumor, cv_normal, logfc}}},
# #           "heatmap_data": {gene: {site_normal: float, site_tumor: float}},
# #           "noise_metrics": {gene: {site: {cv_tumor, cv_normal, logfc}}}
# #         }
# #       },
# #       "log2": {...},
# #       "sample_counts": {site: {tumor: int, normal: int}},
# #       "available_sites": [{id: int, name: str}],
# #       "warning": str,
# #       "debug": dict
# #     }
# #     """
# #     conn = get_connection()
# #     cur = conn.cursor(pymysql.cursors.DictCursor)

# #     cancer_sites = [s.strip() for s in cancer.split(",")]
# #     results = {"raw": {}, "log2": {}}
# #     sample_counts = {}
# #     debug_info = {}
# #     available_sites = []
# #     warning = None

# #     try:
# #         # Fetch available sites
# #         cur.execute("SELECT id, name FROM sites")
# #         available_sites = cur.fetchall()

# #         # Validate analysis type
# #         if analysis_type not in ["ORA", "GSEA"]:
# #             raise HTTPException(status_code=400, detail="Invalid analysis_type. Must be 'ORA' or 'GSEA'.")
# #         if site_analysis_type not in ["pan-cancer", "cancer-specific"]:
# #             raise HTTPException(status_code=400, detail="Invalid site_analysis_type. Must be 'pan-cancer' or 'cancer-specific'.")

# #         # Initialize results structures
# #         for scale in ["raw", "log2"]:
# #             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
# #                 results[scale][norm_method] = {
# #                     "enrichment": [],
# #                     "top_genes": [],
# #                     "gene_stats": {},
# #                     "heatmap_data": {},
# #                     "noise_metrics": {},
# #                 }

# #         # Fetch gene info
# #         selected_gene_set = set()
# #         gene_symbols = {}
# #         gene_ids = []
# #         if genes:
# #             for gene_input in genes:
# #                 cur.execute(
# #                     "SELECT id, gene_symbol, ensembl_id FROM genes WHERE ensembl_id = %s OR gene_symbol = %s",
# #                     (gene_input, gene_input),
# #                 )
# #                 gene_row = cur.fetchone()
# #                 if gene_row:
# #                     gene_ids.append(gene_row["id"])
# #                     gene_symbols[gene_row["id"]] = {
# #                         "symbol": gene_row["gene_symbol"],
# #                         "ensembl_id": gene_row["ensembl_id"]
# #                     }
# #                     selected_gene_set.add(gene_row["gene_symbol"])
# #                 else:
# #                     debug_info[gene_input] = {"error": f"Gene '{gene_input}' not found."}
# #         else:
# #             # Fallback: Select top genes by logFC
# #             warning = "No genes provided. Selecting top differentially expressed genes by logFC."
# #             for site in cancer_sites:
# #                 cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
# #                 site_row = cur.fetchone()
# #                 if not site_row:
# #                     continue
# #                 site_id = site_row["id"]
# #                 if cancer_types:
# #                     cur.execute(
# #                         "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
# #                         (site_id, tuple(cancer_types)),
# #                     )
# #                 else:
# #                     cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
# #                 cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]
# #                 if not cancer_types_list:
# #                     continue
# #                 cur.execute("SELECT id, gene_symbol FROM genes LIMIT 100")  # Limit for performance
# #                 all_genes = cur.fetchall()
# #                 gene_logfc = []
# #                 for gene in all_genes:
# #                     for ct in cancer_types_list:
# #                         cur.execute(
# #                             f"""
# #                             SELECT s.sample_type, e.tpm AS expr
# #                             FROM gene_expressions e
# #                             JOIN samples s ON s.id = e.sample_id
# #                             JOIN cancer_types c ON c.id = s.cancer_type_id
# #                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
# #                             """,
# #                             (site_id, ct, gene["id"]),
# #                         )
# #                         rows = cur.fetchall()
# #                         if not rows:
# #                             continue
# #                         df = pd.DataFrame(rows)
# #                         tumor = pd.Series(df[df["sample_type"].str.lower() == "tumor"]["expr"])
# #                         normal = pd.Series(df[df["sample_type"].str.lower() == "normal"]["expr"])
# #                         tumor_stats = compute_metrics(tumor)
# #                         normal_stats = compute_metrics(normal)
# #                         if tumor_stats and normal_stats and normal_stats["cv"] > 0:
# #                             logfc = math.log2(tumor_stats["cv"] / normal_stats["cv"])
# #                             if not np.isnan(logfc) and abs(logfc) >= logfc_threshold:
# #                                 gene_logfc.append((gene["id"], gene["gene_symbol"], abs(logfc)))
# #                 # Select top 20 genes by |logFC|, or all if fewer
# #                 gene_logfc.sort(key=lambda x: x[2], reverse=True)
# #                 top_genes = gene_logfc[:20]
# #                 if not top_genes:
# #                     warning = f"{warning} No genes met logFC threshold for {site}."
# #                 for gene_id, gene_symbol, _ in top_genes:
# #                     gene_ids.append(gene_id)
# #                     gene_symbols[gene_id] = {"symbol": gene_symbol, "ensembl_id": gene_symbol}
# #                     selected_gene_set.add(gene_symbol)

# #         # Process sample counts and gene stats
# #         for site in cancer_sites:
# #             cur.execute("SELECT id FROM sites WHERE name = %s", (site,))
# #             site_row = cur.fetchone()
# #             if not site_row:
# #                 debug_info[site] = {"error": f"Site '{site}' not found."}
# #                 continue
# #             site_id = site_row["id"]

# #             if cancer_types:
# #                 cur.execute(
# #                     "SELECT tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
# #                     (site_id, tuple(cancer_types)),
# #                 )
# #             else:
# #                 cur.execute("SELECT tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
# #             cancer_types_list = [row["tcga_code"] for row in cur.fetchall()]

# #             if not cancer_types_list:
# #                 debug_info[site] = {"error": f"No cancer types found for site '{site}'."}
# #                 continue

# #             cur.execute(
# #                 """
# #                 SELECT s.sample_type, COUNT(*) AS count
# #                 FROM samples s
# #                 JOIN cancer_types c ON c.id = s.cancer_type_id
# #                 WHERE c.site_id = %s AND c.tcga_code IN %s
# #                 GROUP BY s.sample_type
# #                 """,
# #                 (site_id, tuple(cancer_types_list)),
# #             )
# #             site_counts = {"tumor": 0, "normal": 0}
# #             for row in cur.fetchall():
# #                 stype = row["sample_type"].lower()
# #                 if stype in site_counts:
# #                     site_counts[stype] = row["count"]
# #             sample_counts[site] = site_counts

# #             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
# #                 gene_stats = {}
# #                 heatmap_data = {}
# #                 noise_metrics = {}
# #                 gene_stats_log = {}
# #                 heatmap_data_log = {}
# #                 noise_metrics_log = {}

# #                 for gene_id in gene_ids:
# #                     all_tumor_vals, all_normal_vals = [], []

# #                     for ct in cancer_types_list:
# #                         cur.execute(
# #                             f"""
# #                             SELECT s.sample_type, e.{norm_method} AS expr
# #                             FROM gene_expressions e
# #                             JOIN samples s ON s.id = e.sample_id
# #                             JOIN cancer_types c ON c.id = s.cancer_type_id
# #                             WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s
# #                             """,
# #                             (site_id, ct, gene_id),
# #                         )
# #                         rows = cur.fetchall()
# #                         if not rows:
# #                             debug_info.setdefault(site, {}).setdefault("no_data", []).append(
# #                                 f"No expression data for gene {gene_symbols.get(gene_id, {}).get('symbol', gene_id)} in {norm_method}"
# #                             )
# #                             continue
# #                         df = pd.DataFrame(rows)
# #                         all_tumor_vals.extend(df[df["sample_type"].str.lower() == "tumor"]["expr"].tolist())
# #                         all_normal_vals.extend(df[df["sample_type"].str.lower() == "normal"]["expr"].tolist())

# #                     tumor = pd.Series(all_tumor_vals)
# #                     normal = pd.Series(all_normal_vals)

# #                     tumor_stats = compute_metrics(tumor)
# #                     normal_stats = compute_metrics(normal)
# #                     logfc_raw = np.nan
# #                     if tumor_stats and normal_stats and normal_stats["cv"] > 0:
# #                         logfc_raw = math.log2(tumor_stats["cv"] / normal_stats["cv"])

# #                     df_all = pd.DataFrame({
# #                         "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
# #                         "expr": list(tumor) + list(normal),
# #                     })
# #                     df_all["log2_expr"] = np.log2(df_all["expr"] + 1)

# #                     tumor_log = df_all[df_all["sample_type"] == "tumor"]["log2_expr"]
# #                     normal_log = df_all[df_all["sample_type"] == "normal"]["log2_expr"]

# #                     tumor_stats_log = compute_metrics(tumor_log)
# #                     normal_stats_log = compute_metrics(normal_log)
# #                     logfc_log = np.nan
# #                     if tumor_stats_log and normal_stats_log and normal_stats_log["cv"] > 0:
# #                         logfc_log = math.log2(tumor_stats_log["cv"] / normal_stats_log["cv"])

# #                     gene_symbol = gene_symbols.get(gene_id, {"symbol": f"gene_{gene_id}"}).get("symbol")
# #                     ensembl_id = gene_symbols.get(gene_id, {"ensembl_id": ""}).get("ensembl_id")

# #                     gene_stats[gene_symbol] = {
# #                         **gene_stats.get(gene_symbol, {}),
# #                         site.lower(): {
# #                             "mean_tumor": tumor_stats["mean"] if tumor_stats else None,
# #                             "mean_normal": normal_stats["mean"] if normal_stats else None,
# #                             "cv_tumor": tumor_stats["cv"] if tumor_stats else None,
# #                             "cv_normal": normal_stats["cv"] if normal_stats else None,
# #                             "logfc": logfc_raw,
# #                             "ensembl_id": ensembl_id
# #                         }
# #                     }
# #                     heatmap_data[gene_symbol] = {
# #                         **heatmap_data.get(gene_symbol, {}),
# #                         f"{site}_normal": normal_stats["mean"] if normal_stats else 0,
# #                         f"{site}_tumor": tumor_stats["mean"] if tumor_stats else 0
# #                     }
# #                     noise_metrics[gene_symbol] = {
# #                         **noise_metrics.get(gene_symbol, {}),
# #                         site.lower(): {
# #                             "cv_normal": normal_stats["cv"] if normal_stats else 0,
# #                             "cv_tumor": tumor_stats["cv"] if tumor_stats else 0,
# #                             "logfc": logfc_raw
# #                         }
# #                     }

# #                     gene_stats_log[gene_symbol] = {
# #                         **gene_stats_log.get(gene_symbol, {}),
# #                         site.lower(): {
# #                             "mean_tumor": tumor_stats_log["mean"] if tumor_stats_log else None,
# #                             "mean_normal": normal_stats_log["mean"] if normal_stats_log else None,
# #                             "cv_tumor": tumor_stats_log["cv"] if tumor_stats_log else None,
# #                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else None,
# #                             "logfc": logfc_log,
# #                             "ensembl_id": ensembl_id
# #                         }
# #                     }
# #                     heatmap_data_log[gene_symbol] = {
# #                         **heatmap_data_log.get(gene_symbol, {}),
# #                         f"{site}_normal": normal_stats_log["mean"] if normal_stats_log else 0,
# #                         f"{site}_tumor": tumor_stats_log["mean"] if tumor_stats_log else 0
# #                     }
# #                     noise_metrics_log[gene_symbol] = {
# #                         **noise_metrics_log.get(gene_symbol, {}),
# #                         site.lower(): {
# #                             "cv_normal": normal_stats_log["cv"] if normal_stats_log else 0,
# #                             "cv_tumor": normal_stats_log["cv"] if normal_stats_log else 0,
# #                             "logfc": logfc_log
# #                         }
# #                     }

# #                 results["raw"][norm_method]["gene_stats"].update(gene_stats)
# #                 results["raw"][norm_method]["heatmap_data"].update(heatmap_data)
# #                 results["raw"][norm_method]["noise_metrics"].update(noise_metrics)
# #                 results["log2"][norm_method]["gene_stats"].update(gene_stats_log)
# #                 results["log2"][norm_method]["heatmap_data"].update(heatmap_data_log)
# #                 results["log2"][norm_method]["noise_metrics"].update(noise_metrics_log)

# #         # Perform enrichment if ORA and genes available
# #         enrichment_results = []
# #         if analysis_type == "ORA" and selected_gene_set:
# #             libraries = ['KEGG_2021_Human', 'Reactome_2022', 'GO_Biological_Process_2023', 'GO_Molecular_Function_2023', 'GO_Cellular_Component_2023']
# #             try:
# #                 enr = gp.enrichr(gene_list=list(selected_gene_set),
# #                                 gene_sets=libraries,
# #                                 organism='human',
# #                                 outdir=None)
# #                 res = enr.results
# #                 if res.empty:
# #                     warning = f"No significant pathways found for genes: {list(selected_gene_set)}"
# #                 else:
# #                     # Relaxed p-value filter
# #                     res = res[res["Adjusted P-value"] <= 0.2]
# #                     if res.empty:
# #                         warning = f"No pathways with Adjusted P-value <= 0.2 for genes: {list(selected_gene_set)}"
# #                     res.sort_values("Adjusted P-value", inplace=True)
# #                     res = res.head(top_n)
# #                     for _, row in res.iterrows():
# #                         enrichment_results.append({
# #                             "Term": row["Term"],
# #                             "P-value": row["P-value"],
# #                             "Adjusted P-value": row["Adjusted P-value"],
# #                             "Combined Score": row["Combined Score"],
# #                             "Odds Ratio": row["Odds Ratio"],
# #                             "Genes": row["Genes"].split(";"),
# #                             "GeneSet": row["Gene_set"],
# #                             "Overlap": row["Overlap"],
# #                         })
# #             except Exception as e:
# #                 warning = f"Enrichr analysis failed for genes {list(selected_gene_set)}: {str(e)}"
# #         elif analysis_type == "GSEA":
# #             warning = "GSEA not implemented yet. No enrichment performed."
# #         elif not selected_gene_set:
# #             warning = "No valid genes found for enrichment analysis."

# #         # Assign enrichment to all norm and scales
# #         for scale in ["raw", "log2"]:
# #             for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
# #                 results[scale][norm_method]["enrichment"] = enrichment_results
# #                 results[scale][norm_method]["top_genes"] = list(selected_gene_set)

# #         final_output = {
# #             "raw": results["raw"],
# #             "log2": results["log2"],
# #             "sample_counts": sample_counts,
# #             "available_sites": available_sites,
# #             "warning": warning,
# #             "debug": debug_info if debug_info else None,
# #         }

# #         return JSONResponse(sanitize_floats(final_output))

# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
# #     finally:
# #         conn.close()


# # @app.get("/api/tumor_results")
# # def get_tumor_results(
# #     cancer_site: str = Query(..., description="Cancer site (e.g., Lung, Liver)"),
# #     cancer_types: List[str] = Query(None, description="Optional list of cancer types (TCGA codes)")
# # ):
# #     """
# #     Fetch tumor sample data including DEPTH2 and tITH scores for a given cancer site and optional cancer types.
# #     Returns JSON with structure:
# #     {
# #       "results": [
# #         {
# #           "sample_id": int,
# #           "sample_barcode": str,
# #           "cancer_type": str,
# #           "tpm_depth2": float|null,
# #           "fpkm_depth2": float|null,
# #           "fpkm_uq_depth2": float|null,
# #           "tpm_tith": float|null,
# #           "fpkm_tith": float|null,
# #           "fpkm_uq_tith": float|null
# #         }
# #       ],
# #       "sample_counts": {
# #         "tumor": int,
# #         "normal": int
# #       },
# #       "error": str|null
# #     }
# #     """
# #     conn = get_connection()
# #     cur = conn.cursor(pymysql.cursors.DictCursor)
    
# #     try:
# #         # Fetch site ID
# #         cur.execute("SELECT id FROM sites WHERE name = %s", (cancer_site,))
# #         site_row = cur.fetchone()
# #         if not site_row:
# #             raise HTTPException(status_code=404, detail=f"Cancer site '{cancer_site}' not found.")
# #         site_id = site_row["id"]

# #         # Fetch cancer types
# #         if cancer_types:
# #             cur.execute(
# #                 "SELECT id, tcga_code FROM cancer_types WHERE site_id = %s AND tcga_code IN %s",
# #                 (site_id, tuple(cancer_types)),
# #             )
# #         else:
# #             cur.execute("SELECT id, tcga_code FROM cancer_types WHERE site_id = %s", (site_id,))
# #         cancer_type_rows = cur.fetchall()
# #         if not cancer_type_rows:
# #             raise HTTPException(status_code=404, detail=f"No cancer types found for site '{cancer_site}'.")
        
# #         cancer_type_ids = [row["id"] for row in cancer_type_rows]
# #         cancer_type_map = {row["id"]: row["tcga_code"] for row in cancer_type_rows}

# #         # Fetch sample counts
# #         cur.execute(
# #             """
# #             SELECT s.sample_type, COUNT(*) AS count
# #             FROM samples s
# #             JOIN cancer_types c ON c.id = s.cancer_type_id
# #             WHERE c.site_id = %s AND c.tcga_code IN %s
# #             GROUP BY s.sample_type
# #             """,
# #             (site_id, tuple(cancer_type_map.values())),
# #         )
# #         sample_counts = {"tumor": 0, "normal": 0}
# #         for row in cur.fetchall():
# #             stype = row["sample_type"].lower()
# #             if stype in sample_counts:
# #                 sample_counts[stype] = row["count"]

# #         # Fetch tumor samples with DEPTH2 and tITH scores
# #         cur.execute(
# #             """
# #             SELECT 
# #                 s.id AS sample_id,
# #                 s.sample_barcode,
# #                 s.cancer_type_id,
# #                 d.tpm AS tpm_depth2,
# #                 d.fpkm AS fpkm_depth2,
# #                 d.fpkm_uq AS fpkm_uq_depth2,
# #                 t.tpm AS tpm_tith,
# #                 t.fpkm AS fpkm_tith,
# #                 t.fpkm_uq AS fpkm_uq_tith
# #             FROM samples s
# #             JOIN cancer_types c ON c.id = s.cancer_type_id
# #             LEFT JOIN depth2_scores d ON d.sample_id = s.id
# #             LEFT JOIN tith_scores t ON t.sample_id = s.id
# #             WHERE c.site_id = %s 
# #             AND c.id IN %s 
# #             AND s.sample_type = 'tumor'
# #             """,
# #             (site_id, tuple(cancer_type_ids)),
# #         )
# #         rows = cur.fetchall()

# #         results = [
# #             {
# #                 "sample_id": row["sample_id"],
# #                 "sample_barcode": row["sample_barcode"],
# #                 "cancer_type": cancer_type_map.get(row["cancer_type_id"], "Unknown"),
# #                 "tpm_depth2": row["tpm_depth2"],
# #                 "fpkm_depth2": row["fpkm_depth2"],
# #                 "fpkm_uq_depth2": row["fpkm_uq_depth2"],
# #                 "tpm_tith": row["tpm_tith"],
# #                 "fpkm_tith": row["fpkm_tith"],
# #                 "fpkm_uq_tith": row["fpkm_uq_tith"],
# #             }
# #             for row in rows
# #         ]

# #         final_output = {
# #             "results": results,
# #             "sample_counts": sample_counts,
# #             "error": None
# #         }
# #         return JSONResponse(sanitize_floats(final_output))

# #     except Exception as e:
# #         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
# #     finally:
# #         conn.close()

# # # ------------------------------
# # # Entry Point
# # # ------------------------------
# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=5001)
    
# # def sanitize_floats(obj):
# #     if isinstance(obj, dict):
# #         return {k: sanitize_floats(v) for k, v in obj.items()}
# #     elif isinstance(obj, list):
# #         return [sanitize_floats(v) for v in obj]
# #     elif isinstance(obj, float):
# #         if math.isnan(obj) or math.isinf(obj):
# #             return None
# #         return obj
# #     return obj

# # def get_connection():
# #     return pymysql.connect(**DB_CONFIG)

# # def compute_metrics(values: pd.Series):
# #     if len(values) == 0:
# #         return None
# #     mean = values.mean()
# #     std = values.std(ddof=1)
# #     mad = (values - mean).abs().mean()
# #     cv = (std / mean) * 100 if mean != 0 else np.nan
# #     cv2 = cv**2 if not np.isnan(cv) else np.nan
# #     return {"mean": mean, "std": std, "mad": mad, "cv": cv, "cv_squared": cv2}

# @app.get("/api/gene_noise")
# def get_gene_noise(
#     cancer_site: str = Query(..., description="Cancer site (e.g., 'Lung', 'Liver', 'Adrenal Gland')"),
#     cancer_type: List[str] = Query(None, description="Cancer type (optional: if not provided, use all types for site)"),
#     gene_ids: List[str] = Query(..., description="One or more Ensembl IDs or gene symbols (e.g., 'TP53')"),
# ):
#     print(cancer_type)
#     """
#     For a given cancer site and list of gene IDs (Ensembl IDs or gene symbols), compute mean, std, mad, cv, cv², and logFC
#     between tumor vs normal samples across selected cancer types.
#     Returns sample counts, processed data, and debug information.
#     """
#     start_time = time.time()
#     conn = get_connection()
#     cur = conn.cursor(pymysql.cursors.DictCursor)
#     cur.execute("""
#                 SELECT id 
#                 FROM sites 
#                 where
#                 name = %s
#             """, (cancer_site,))
#     cancer_site_id = [row["id"] for row in cur.fetchall()]
#     print(cancer_site_id)
#     print(cancer_type)

# #     cur.execute("""
# # SELECT count(*) 
# # FROM samples 
# # WHERE cancer_type_id IN (
# #     SELECT id 
# #     FROM cancer_types 
# #     WHERE site_id IN (
# #         SELECT id 
# #         FROM Sites
# #         WHERE name = 'Colorectal' 
# #         AND tcga_code = 'TCGA-READ'
# #     )
# # )
# # AND sample_type='tumor'
# #                  """)
    
    
#     # cancer_site_id = [row["id"] for row in cur.fetchall()]
#     # print(cur.fetchall())

#     results = {"raw": {}, "log2": {}, "sample_counts": {}, "cancer_types_used": [], "debug": {}}
#     try:
#         # Get cancer types based on site
#         if cancer_type:
#             print(cancer_type)
#             cur.execute("SELECT tcga_code FROM cancer_types WHERE tcga_code in %s", (cancer_type,))
#             # print(cur.fetchall())
#         else:
#             cur.execute("""
#                 SELECT ct.tcga_code 
#                 FROM cancer_types ct
#                 JOIN Sites cs ON cs.id = ct.site_id
#                 WHERE cs.name = %s
#             """, (cancer_site,))
#         cancer_types = [row["tcga_code"] for row in cur.fetchall()]
#         print(cancer_types)
#         if not cancer_types:
#             return {"error": f"No cancer types found for site '{cancer_site}'."}

#         results["cancer_types_used"] = cancer_types
    
#         # Initialize sample counts
#         sample_counts = {"tumor": 0, "normal": 0}
        
#         if cancer_type and len(cancer_type) == 1:
#             cur.execute("""
#                 SELECT s.sample_type, COUNT(*) as count
#                 FROM samples s
#                 JOIN cancer_types c ON c.id = s.cancer_type_id
#                 WHERE c.tcga_code = %s AND c.site_id = %s
#                 GROUP BY s.sample_type
#             """, (cancer_type, cancer_site_id))
#             # print(cur.fetchall)
#             for row in cur.fetchall():
#                 sample_type = row["sample_type"].lower()
#                 if sample_type == "tumor":
#                     sample_counts["tumor"] = row["count"]
#                 elif sample_type == "normal":
#                     sample_counts["normal"] = row["count"]
#         else:
#             # print((cancer_types))
#             for ct in cancer_types:
#                 cur.execute("""
#                     SELECT s.sample_type, COUNT(*) as count
#                     FROM samples s
#                     JOIN cancer_types c ON c.id = s.cancer_type_id
#                     WHERE c.tcga_code = %s AND c.site_id = %s
#                     GROUP BY s.sample_type
#                 """, (ct, cancer_site_id))
#                 print(cur.fetchall)
#                 for row in cur.fetchall():
#                     sample_type = row["sample_type"].lower()
#                     if sample_type == "tumor":
#                         sample_counts["tumor"] += row["count"]
#                     elif sample_type == "normal":
#                         sample_counts["normal"] += row["count"]
            
#         results["sample_counts"][cancer_site] = sample_counts

#         for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
#             results["raw"][norm_method] = {}
#             results["log2"][norm_method] = {}
#             results["debug"][norm_method] = {}

#             for gene_input in gene_ids:
#                 # Try gene_input as both Ensembl ID and gene symbol
#                 cur.execute("""
#                     SELECT id, ensembl_id, gene_symbol 
#                     FROM genes 
#                     WHERE ensembl_id = %s OR gene_symbol = %s
#                 """, (gene_input, gene_input))
#                 gene_row = cur.fetchone()
#                 if not gene_row:
#                     results["debug"][norm_method][gene_input] = {
#                         "error": f"Gene '{gene_input}' not found in database."
#                     }
#                     continue
#                 gene_id = gene_row["id"]
#                 ensembl_id = gene_row["ensembl_id"]
#                 gene_symbol = gene_row["gene_symbol"]

#                 all_tumor_vals, all_normal_vals = [], []

#                 for ct in cancer_types:
#                     sql = f"""
#                         SELECT s.sample_type, e.{norm_method} AS expr
#                         FROM gene_expressions e
#                         JOIN samples s ON s.id = e.sample_id
#                         JOIN cancer_types c ON c.id = s.cancer_type_id
#                         WHERE c.site_id = %s AND c.tcga_code = %s AND e.gene_id = %s  
#                     """
#                     cur.execute(sql, (cancer_site_id, ct, gene_id ))
#                     rows = cur.fetchall()
#                     if not rows:
#                         continue

#                     df = pd.DataFrame(rows)
#                     all_tumor_vals.extend(df[df["sample_type"].str.lower().eq("tumor")]["expr"].tolist())
#                     all_normal_vals.extend(df[df["sample_type"].str.lower().eq("normal")]["expr"].tolist())

#                 results["debug"][norm_method][gene_input] = {
#                     "tumor_samples": len(all_tumor_vals),
#                     "normal_samples": len(all_normal_vals)
#                 }

#                 if not all_tumor_vals and not all_normal_vals:
#                     results["debug"][norm_method][gene_input]["error"] = f"No expression data found for gene '{gene_input}' ({norm_method})."
#                     continue

#                 tumor = pd.Series(all_tumor_vals)
#                 normal = pd.Series(all_normal_vals)

#                 # RAW METRICS
#                 tumor_stats = compute_metrics(tumor)
#                 # if len(all_normal_vals) == 0:
#                 #     normal_stats = 0
#                 # else:
#                 normal_stats = compute_metrics(normal)

#                 logfc = np.nan
#                 if tumor_stats and normal_stats and normal_stats["mean"] > 0:
#                     logfc = math.log2(tumor_stats["mean"] / normal_stats["mean"])

#                 results["raw"][norm_method][ensembl_id] = {
#                     "gene_symbol": gene_symbol,
#                     "mean_tumor": tumor_stats["mean"] if tumor_stats else np.nan,
#                     "mean_normal": normal_stats["mean"] if normal_stats else np.nan,
#                     "std_tumor": tumor_stats["std"] if tumor_stats else np.nan,
#                     "std_normal": normal_stats["std"] if normal_stats else np.nan,
#                     "mad_tumor": tumor_stats["mad"] if tumor_stats else np.nan,
#                     "mad_normal": normal_stats["mad"] if normal_stats else np.nan,
#                     "cv_tumor": tumor_stats["cv"] if tumor_stats else np.nan,
#                     "cv_normal": normal_stats["cv"] if normal_stats else np.nan,
#                     "cv_squared_tumor": tumor_stats["cv_squared"] if tumor_stats else np.nan,
#                     "cv_squared_normal": normal_stats["cv_squared"] if normal_stats else np.nan,
#                     "logfc": logfc,
#                 }

#                 # LOG2 METRICS
#                 df = pd.DataFrame({
#                     "sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
#                     "expr": list(tumor) + list(normal)
#                 })
#                 df["log2_expr"] = np.log2(df["expr"] + 1)

#                 tumor_log = df[df["sample_type"].eq("tumor")]["log2_expr"]
#                 normal_log = df[df["sample_type"].eq("normal")]["log2_expr"]

#                 tumor_stats_log = compute_metrics(tumor_log)
#                 normal_stats_log = compute_metrics(normal_log)

#                 logfc_log = np.nan
#                 if tumor_stats_log and normal_stats_log:
#                     logfc_log = tumor_stats_log["mean"] - normal_stats_log["mean"]

#                 results["log2"][norm_method][ensembl_id] = {
#                     "gene_symbol": gene_symbol,
#                     "mean_tumor": tumor_stats_log["mean"] if tumor_stats_log else np.nan,
#                     "mean_normal": normal_stats_log["mean"] if normal_stats_log else np.nan,
#                     "std_tumor": tumor_stats_log["std"] if tumor_stats_log else np.nan,
#                     "std_normal": normal_stats_log["std"] if normal_stats_log else np.nan,
#                     "mad_tumor": tumor_stats_log["mad"] if tumor_stats_log else np.nan,
#                     "mad_normal": normal_stats_log["mad"] if normal_stats_log else np.nan,
#                     "cv_tumor": tumor_stats_log["cv"] if tumor_stats_log else np.nan,
#                     "cv_normal": normal_stats_log["cv"] if normal_stats_log else np.nan,
#                     "cv_squared_tumor": tumor_stats_log["cv_squared"] if tumor_stats_log else np.nan,
#                     "cv_squared_normal": normal_stats_log["cv_squared"] if normal_stats_log else np.nan,
#                     "logfc": logfc_log,
#                 }

#         results["execution_time_sec"] = round(time.time() - start_time, 3)
#     except Exception as e:
#         results["error"] = f"An unexpected error occurred: {str(e)}"
#     finally:
#         conn.close()
#     print(results)
#     results = sanitize_floats(results)
#     return JSONResponse(results)

# # if __name__ == "__main__":
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=5001)

# # def get_connection():
# #     return pymysql.connect(**DB_CONFIG)

# # # === Helper: compute metrics ===
# # def compute_metrics(values: pd.Series):
# #     if len(values) == 0:
# #         return None
# #     mean = values.mean()
# #     std = values.std(ddof=1)
# #     mad = (values - mean).abs().mean()
# #     cv = std / mean if mean != 0 else np.nan
# #     cv2 = cv**2 if not np.isnan(cv) else np.nan
# #     return {"mean": mean, "std": std, "mad": mad, "cv": cv, "cv_squared": cv2}


# # # === MAIN ENDPOINT ===
# # @app.get("/api/gene_noise")
# # def get_gene_noise(
# #     cancer_site: str = Query(..., description="Cancer site (e.g., 'Lung', 'Liver', 'Adrenal Gland')"),
# #     cancer_type: Optional[str] = Query(None, description="Cancer type (optional: if not provided, use all types for site)"),
# #     gene_ids: List[str] = Query(..., description="One or more Ensembl IDs"),
# # ):
# #     """
# #     For a given cancer site and list of gene Ensembl IDs, compute mean, std, mad, cv, cv², and logFC
# #     between tumor vs normal samples across selected cancer types.
# #     If no cancer_type is given, uses all cancer types associated with that site.
# #     """
# #     start_time = time.time()
# #     conn = get_connection()
# #     cur = conn.cursor()

# #     results = {"raw": {}, "log2": {}}

# #     # === Get cancer types based on site ===
# #     if cancer_type:
# #         cur.execute("SELECT name FROM cancer_types WHERE name = %s", (cancer_type,))
# #     else:
# #         cur.execute("""
# #             SELECT ct.name 
# #             FROM cancer_types ct
# #             JOIN Sites cs ON cs.id = ct.site_id
# #             WHERE cs.name = %s
# #         """, (cancer_site,))
# #     cancer_types = [row["name"] for row in cur.fetchall()]
    
# #     if not cancer_types:
# #         conn.close()
# #         return {"error": f"No cancer types found for site '{cancer_site}'."}

# #     for norm_method in ["tpm", "fpkm", "fpkm_uq"]:
# #         results["raw"][norm_method] = {}
# #         results["log2"][norm_method] = {}

# #         for gene in gene_ids:
# #             cur.execute("SELECT id FROM genes WHERE ensembl_id = %s", (gene,))
# #             gene_row = cur.fetchone()
# #             if not gene_row:
# #                 continue
# #             gene_id = gene_row["id"]

# #             all_tumor_vals, all_normal_vals = [], []

# #             for ct in cancer_types:
# #                 sql = f"""
# #                     SELECT s.sample_type, e.{norm_method} AS expr
# #                     FROM gene_expressions e
# #                     JOIN samples s ON s.id = e.sample_id
# #                     JOIN cancer_types c ON c.id = s.cancer_type_id
# #                     WHERE c.name = %s AND e.gene_id = %s;
# #                 """
# #                 cur.execute(sql, (ct, gene_id))
# #                 rows = cur.fetchall()
# #                 if not rows:
# #                     continue

# #                 df = pd.DataFrame(rows)
# #                 all_tumor_vals.extend(df[df["sample_type"].str.lower().eq("tumor")]["expr"].tolist())
# #                 all_normal_vals.extend(df[df["sample_type"].str.lower().eq("normal")]["expr"].tolist())

# #             if not all_tumor_vals and not all_normal_vals:
# #                 continue

# #             tumor = pd.Series(all_tumor_vals)
# #             normal = pd.Series(all_normal_vals)

# #             # === RAW METRICS ===
# #             tumor_stats = compute_metrics(tumor)
# #             normal_stats = compute_metrics(normal)

# #             logfc = np.nan
# #             if tumor_stats and normal_stats and normal_stats["mean"] > 0:
# #                 logfc = math.log2(tumor_stats["mean"] / normal_stats["mean"])

# #             results["raw"][norm_method][gene] = {
# #                 "mean_tumor": tumor_stats["mean"] if tumor_stats else np.nan,
# #                 "mean_normal": normal_stats["mean"] if normal_stats else np.nan,
# #                 "std_tumor": tumor_stats["std"] if tumor_stats else np.nan,
# #                 "std_normal": normal_stats["std"] if normal_stats else np.nan,
# #                 "mad_tumor": tumor_stats["mad"] if tumor_stats else np.nan,
# #                 "mad_normal": normal_stats["mad"] if normal_stats else np.nan,
# #                 "cv_tumor": tumor_stats["cv"] if tumor_stats else np.nan,
# #                 "cv_normal": normal_stats["cv"] if normal_stats else np.nan,
# #                 "cv2_tumor": tumor_stats["cv_squared"] if tumor_stats else np.nan,
# #                 "cv2_normal": normal_stats["cv_squared"] if normal_stats else np.nan,
# #                 "logfc": logfc,
# #             }

# #             # === LOG2 METRICS ===
# #             df = pd.DataFrame({"sample_type": ["tumor"] * len(tumor) + ["normal"] * len(normal),
# #                                "expr": pd.concat([tumor, normal])})
# #             df["log2_expr"] = np.log2(df["expr"] + 1)

# #             tumor_log = df[df["sample_type"].eq("tumor")]["log2_expr"]
# #             normal_log = df[df["sample_type"].eq("normal")]["log2_expr"]

# #             tumor_stats_log = compute_metrics(tumor_log)
# #             normal_stats_log = compute_metrics(normal_log)

# #             logfc_log = np.nan
# #             if tumor_stats_log and normal_stats_log:
# #                 logfc_log = tumor_stats_log["mean"] - normal_stats_log["mean"]

# #             results["log2"][norm_method][gene] = {
# #                 "mean_tumor": tumor_stats_log["mean"] if tumor_stats_log else np.nan,
# #                 "mean_normal": normal_stats_log["mean"] if normal_stats_log else np.nan,
# #                 "std_tumor": tumor_stats_log["std"] if tumor_stats_log else np.nan,
# #                 "std_normal": normal_stats_log["std"] if normal_stats_log else np.nan,
# #                 "mad_tumor": tumor_stats_log["mad"] if tumor_stats_log else np.nan,
# #                 "mad_normal": normal_stats_log["mad"] if normal_stats_log else np.nan,
# #                 "cv_tumor": tumor_stats_log["cv"] if tumor_stats_log else np.nan,
# #                 "cv_normal": normal_stats_log["cv"] if normal_stats_log else np.nan,
# #                 "cv2_tumor": tumor_stats_log["cv_squared"] if tumor_stats_log else np.nan,
# #                 "cv2_normal": normal_stats_log["cv_squared"] if normal_stats_log else np.nan,
# #                 "logfc": logfc_log,
# #             }


# #     conn.close()
# #     # results["execution_time_sec"] = round(time.time() - start_time, 3)
# #     # results["cancer_types_used"] = cancer_types
# #     print(results)
# #     return JSONResponse(results)

# # if __name__ == '__main__':
# #     import uvicorn
# #     uvicorn.run(app, host="0.0.0.0", port=5001)
