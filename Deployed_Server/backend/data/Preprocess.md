# GDC Data Preprocessing Pipeline

This guide provides step-by-step instructions to download RNA-seq gene expression data from the Genomic Data Commons (GDC) API and populate a MariaDB database for use with the TCNA platform.

## Prerequisites

To run this pipeline, ensure the following is installed on your system:

- Python 3.11+
- MariaDB 12.0.2
- Required Python packages: `pandas`, `requests`, `pymysql`, `tqdm`

## Set up

Once the above requirements are fulfilled, you can populate the database by following these steps:

### Step 1: Download GDC Data

```sh
# Navigate to the scripts directory
cd backend/scripts/data

# Run the GDC preprocessing script
python preprocess.py
```

It downloads RNA-seq data from GDC API and creates CSV files

**Output:** CSV files will be saved in `tcga_csvs/{cancer_site}/` directory:
- `tumor_tpm.csv`, `tumor_fpkm.csv`, `tumor_fpkm_uq.csv`
- `normal_tpm.csv`, `normal_fpkm.csv`, `normal_fpkm_uq.csv`

**Time:** 2-6 hours for all cancer sites

### Step 2: Move CSV Files

```sh
# Copy CSV files to backend data directory
cp -r tcga_csvs/* backend/data/raw/
```

**Expected structure:**
```
backend/data/raw/
├── Adrenal Gland/
│   ├── tumor_tpm.csv
│   ├── tumor_fpkm.csv
│   ├── tumor_fpkm_uq.csv
│   ├── normal_tpm.csv
│   ├── normal_fpkm.csv
│   └── normal_fpkm_uq.csv
├── Bladder/
│   └── ...
└── ...
```

### Step 3: Create MariaDB Database and Tables

```sh
# Navigate to scripts directory
cd backend/scripts/data

# Run the SQL file to create database and tables
mysql -u your_username -p < create_tables.sql
```

**What it does:** Creates the `cancer_db` database and all required tables:
- `genes` - Gene metadata
- `sites` - Cancer site information
- `cancer_types` - TCGA project codes
- `samples` - Sample metadata
- `gene_expressions` - Expression data

### Step 4: Configure Database Connection

```sh
# Edit db_conn.py and update database credentials
# Update the following in backend/scripts/db_conn.py:
# - host
# - user
# - password
# - database
# - port (if different from default)
```

### Step 5: Enable Local File Loading

```sh
# Connect to MariaDB
mysql -u your_username -p

# Enable local file loading
SET GLOBAL local_infile = 1;
```

### Step 6: Test with Example File (Optional)

```sh
# Navigate to scripts directory
cd backend/scripts

# Run the example function to see how a single file is populated
python populate_db.py --example
```

**What it does:** Demonstrates the workflow using `backend/data/raw/lymph/tumor_tpm.csv` as an example:
- Shows how CSV files are loaded and processed
- Demonstrates gene and sample extraction
- Illustrates the wide-to-long format conversion
- Shows ID mapping and database insertion


### Step 7: Populate Database Tables

```sh
# Navigate to scripts directory
cd backend/scripts

# Run the population script
python populate_db.py
```

**What it does:** 
- Automatically detects all cancer sites from `backend/data/raw/` directory
- Extracts and populates `genes` table from CSV files
- Extracts and populates `sites` table from directory structure
- Extracts and populates `cancer_types` table from sample barcodes
- Extracts and populates `samples` table from CSV headers
- Loads expression data into `gene_expressions` table

**Note:** The script automatically processes all cancer sites found in the data directory. No manual configuration needed.

### Step 8: Verify Data

```sh
# Connect to MariaDB
mysql -u your_username -p cancer_db

# Run verification queries
SELECT COUNT(*) FROM gene_expressions;

SELECT s.name, COUNT(*) as count
FROM gene_expressions ge
JOIN samples s ON ge.sample_id = s.id
JOIN sites st ON s.site_id = st.id
GROUP BY st.name;
```

### Step 9: Create Indexes (Optional but Recommended)

```sh
# Run the indexes SQL file
mysql -u your_username -p cancer_db < db_indexes.sql
```

Or run manually:
```sql
ALTER TABLE gene_expressions 
    ADD INDEX idx_gene_sample (gene_id, sample_id),
    ADD INDEX idx_sample_gene (sample_id, gene_id);
```

## Example Workflow

To understand how the population script works, you can run the example function:

```sh
cd backend/scripts/data
mysql -u your_username -p < create_tables.sql
python populate_db.py --example
```

This will demonstrate the complete workflow using `backend/data/raw/lymph/tumor_tpm.csv` as a reference file, showing:
- Database tables creation
- CSV file loading
- Gene extraction and insertion
- Sample extraction and insertion  
- Data format conversion (wide to long)
- ID mapping and expression data insertion


