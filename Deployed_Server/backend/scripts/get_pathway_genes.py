from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Set
import requests
import re

def fetch_kegg_genes(pathway_id: str) -> List[str]:
    """Fetch all genes in a KEGG pathway using REST API."""
    try:
        resp = requests.get(f"https://rest.kegg.jp/get/{pathway_id}", timeout=10)
        resp.raise_for_status()
        text = resp.text
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch KEGG genes: {str(e)}")

    # Example lines: "10000  AKT3; AKT serine/threonine kinase 3"
    genes = re.findall(r"\d+\s+([A-Za-z0-9_-]+);", text)
    return list(sorted(set(genes)))


def fetch_go_genes(go_id: str, taxon: int = 9606) -> List[str]:
    """Fetch all human gene symbols annotated with a GO term from QuickGO."""
    url = f"https://www.ebi.ac.uk/QuickGO/services/annotation/search?goId={go_id}&taxonId={taxon}"
    try:
        resp = requests.get(url, timeout=10, headers={"Accept": "application/json"})
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch GO genes: {str(e)}")

    # Extract unique symbols
    genes = {r["symbol"].strip() for r in data.get("results", []) if r.get("symbol")}
    return list(sorted(genes))