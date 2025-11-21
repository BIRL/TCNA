import requests
import pandas as pd
import io
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import os

# GDC API endpoints
FILES_ENDPOINT = "https://api.gdc.cancer.gov/files"
DATA_ENDPOINT = "https://api.gdc.cancer.gov/data"

# Output directory
OUTPUT_DIR = os.path.join(os.getcwd(), 'tcga_csvs')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# get files for selected primary_sites (update accordingly for all)
def process_file(file_id, sample_id, file_name, data_endpoint):
    try:
        response = requests.get(f"{data_endpoint}/{file_id}", stream=True, timeout=30)
        response.raise_for_status()

        content = response.content[:4]
        is_gzipped = content.startswith(b'\x1f\x8b')

        if is_gzipped:
            with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode='rb') as f:
                df = pd.read_csv(f, sep='\t', comment='#')
        else:
            df = pd.read_csv(io.BytesIO(response.content), sep='\t', comment='#')

        df = df[~df['gene_id'].str.startswith('N_')]

        required_columns = ['gene_id', 'gene_name', 'tpm_unstranded', 'fpkm_unstranded', 'fpkm_uq_unstranded']
        if not all(col in df.columns for col in required_columns):
            print(f"Skipping file {file_id}: Missing required columns")
            return None, None, None, None, None

        gene_ids = df['gene_id'].tolist()
        gene_names = df['gene_name'].tolist()
        tpm_series = df['tpm_unstranded'].rename(sample_id)
        fpkm_series = df['fpkm_unstranded'].rename(sample_id)
        fpkm_uq_series = df['fpkm_uq_unstranded'].rename(sample_id)

        print(f"Processed file: {file_name} as sample_id: {sample_id}")
        return tpm_series, fpkm_series, fpkm_uq_series, gene_ids, gene_names
    except Exception as e:
        print(f"Error processing file {file_id}: {e}")
        return None, None, None, None, None

def get_rna_seq_metadata(primary_site):
    params = {
        "filters": {
            "op": "and",
            "content": [
                # {"op": "=", "content": {"field": "cases.project.program.name", "value": "TCGA"}},
                # {"op": "=", "content": {"field": "cases.project.project_id", "value": project_id}},
                {"op": "=", "content": {"field": "cases.primary_site", "value": primary_site}},
                {"op": "=", "content": {"field": "data_category", "value": "Transcriptome Profiling"}},
                {"op": "=", "content": {"field": "data_type", "value": "Gene Expression Quantification"}},
                {"op": "=", "content": {"field": "experimental_strategy", "value": "RNA-Seq"}},
                {"op": "=", "content": {"field": "analysis.workflow_type", "value": "STAR - Counts"}},
                {"op": "=", "content": {"field": "access", "value": "open"}},
                {"op": "=", "content": {"field": "data_format", "value": "TSV"}}
            ]
        },
        "fields": "file_id,file_name,cases.samples.submitter_id,cases.samples.sample_type,cases.project.project_id",
        "format": "JSON",
        "size": 2000
    }

    try:
        response = requests.post(FILES_ENDPOINT, json=params, timeout=30)
        response.raise_for_status()
        data = response.json()["data"]["hits"]
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return pd.DataFrame()

    sample_data = []
    for hit in data:
        file_id = hit.get("file_id")
        file_name = hit.get("file_name")
        for case in hit.get("cases", []):
            project_id = case.get("project", {}).get("project_id")
            for sample in case.get("samples", []):
                sample_id = sample.get("submitter_id")
                sample_type = sample.get("sample_type")
                if sample_id and sample_type:
                    sample_data.append({
                        "sample_barcode": sample_id,
                        "sample_type": sample_type,
                        "tcga_code": project_id,
                        "file_id": file_id,
                        "file_name": file_name
                    })

    return pd.DataFrame(sample_data)

def generate_expression_csvs(cancer_types):
    for cancer_type in cancer_types:
        print(f"\n🔬 Processing {cancer_type}...")
        sample_df = get_rna_seq_metadata(cancer_type)
        if sample_df.empty:
            print(f"No metadata found for {cancer_type}. Skipping.")
            continue

        tumor_sample_df = sample_df[sample_df["sample_type"].str.lower().str.contains("tumor")]
        normal_sample_df = sample_df[sample_df["sample_type"].str.lower().str.contains("normal")]
        print("found: ", len(sample_df))
        for label, group_df in [("tumor", tumor_sample_df), ("normal", normal_sample_df)]:
            if group_df.empty:
                print(f"No {label} samples found for {cancer_type}")
                continue

            print(f"\nDownloading and processing {label} RNA-seq data...")
            tpm_data, fpkm_data, fpkm_uq_data = [], [], []
            new_gene_ids, new_gene_names = None, None

            with ThreadPoolExecutor(max_workers=25) as executor:
                future_to_file = {
                    executor.submit(
                        partial(process_file, row["file_id"], row["sample_barcode"], row["file_name"], DATA_ENDPOINT)
                    ): row["sample_barcode"]
                    for _, row in group_df.iterrows()
                }

                for future in as_completed(future_to_file):
                    tpm_series, fpkm_series, fpkm_uq_series, gene_ids, gene_names = future.result()
                    if tpm_series is not None:
                        tpm_data.append(tpm_series)
                        fpkm_data.append(fpkm_series)
                        fpkm_uq_data.append(fpkm_uq_series)
                        if new_gene_ids is None and gene_ids is not None:
                            new_gene_ids = gene_ids
                            new_gene_names = gene_names

            if not tpm_data:
                print(f"No valid {label} data for {cancer_type}")
                continue

            # Construct matrices
            tpm_matrix = pd.concat(tpm_data, axis=1)
            fpkm_matrix = pd.concat(fpkm_data, axis=1)
            fpkm_uq_matrix = pd.concat(fpkm_uq_data, axis=1)

            tpm_matrix.insert(0, "gene_name", new_gene_names)
            tpm_matrix.insert(0, "gene_id", new_gene_ids)

            fpkm_matrix.insert(0, "gene_name", new_gene_names)
            fpkm_matrix.insert(0, "gene_id", new_gene_ids)

            fpkm_uq_matrix.insert(0, "gene_name", new_gene_names)
            fpkm_uq_matrix.insert(0, "gene_id", new_gene_ids)

            # Save to files
            code = cancer_type.replace("TCGA-", "")
            tpm_path = os.path.join(OUTPUT_DIR, f"TCGA-{code}_{label}_tpm.csv")
            fpkm_path = os.path.join(OUTPUT_DIR, f"TCGA-{code}_{label}_fpkm.csv")
            fpkm_uq_path = os.path.join(OUTPUT_DIR, f"TCGA-{code}_{label}_fpkm_uq.csv")

            tpm_matrix.to_csv(tpm_path, index=False)
            fpkm_matrix.to_csv(fpkm_path, index=False)
            fpkm_uq_matrix.to_csv(fpkm_uq_path, index=False)


            print(f"Saved {label} CSVs for {cancer_type}:\n{tpm_path}\n{fpkm_path}\n{fpkm_uq_path}")

if __name__ == "__main__":
    import time
    start = time.time()
    generate_expression_csvs(["Thymus"])  # Add more TCGA types here
    print(f"\nTotal time: {time.time() - start:.2f} seconds")
