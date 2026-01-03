# TCNA - The Cancer Noise Atlas
TCNA is a multi-omics noise analysis tool for exploring genomic variability across multiple spatio-temporal scales. It supports noise analysis at genic, pathway, and tumor level across 26 cancer sites using GDC datasets or user uploaded data.
The frontend is built with Vite, TypeScript, React, shadcn-ui, Tailwind CSS and the backend is built with Python FastAPI and RESTful API.

# Set up
To run this on your local system, ensure the following is downloaded on your system:  

- Node.js and npm (if not use [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating))
- Python 3.11+
- MariaDB 12.0.2


Once the above requirements are fulfilled, you can run the website by following these steps:

# GDC Data Preprocessing Pipeline

First, we will have to download RNA-seq gene expression data from the Genomic Data Commons (GDC) API and populate a MariaDB database for use with the TCNA platform.

### Step 1: Download GDC Data

```sh
# Navigate to the scripts directory
cd backend/scripts/data

# Run the GDC preprocessing script
python preprocess.py
```

It downloads RNA-seq data from GDC API and creates CSV files

**Output:** CSV files will be saved in `data/raw/{cancer_site}/` directory:
- `tumor_tpm.csv`, `tumor_fpkm.csv`, `tumor_fpkm_uq.csv`
- `normal_tpm.csv`, `normal_fpkm.csv`, `normal_fpkm_uq.csv`

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

### Step 2: Create MariaDB Database and Tables

```sh
# Navigate to scripts directory
cd backend/scripts/data

# Run the SQL file to create database and tables
mysql -u your_username -p < create_tables.sql
```

### Step 2: Configure Database Connection

```sh
# Edit db_conn.py and update database credentials
# Update the following in backend/scripts/db_conn.py:
# - host
# - user
# - password
# - database
# - port (if different from default)
```

### Step 3: Enable Local File Loading

```sh
# Connect to MariaDB
mysql -u your_username -p

# Enable local file loading
SET GLOBAL local_infile = 1;
```

### Step 4: Test with Example File (Optional)

```sh
# Navigate to scripts directory
cd backend/scripts

# Run the example function to see how a single file is populated
python populate_db.py --example
```

This demonstrates the workflow using `backend/data/raw/Thymus/tumor_tpm.csv` as an example, to see how the population works:
- Shows how CSV files are loaded and processed
- Demonstrates gene and sample extraction
- Illustrates the wide-to-long format conversion
- Shows ID mapping and database insertion

### Step 5: Create Indexes (Optional but Recommended)

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


### Backend Setup:
```sh
# Step 1: Clone the repository using the project's Git URL.
git clone <GIT_URL>

# Step 2: Navigate to the project directory.
cd <TCNA/Deployed_Server/backend/scripts>

# Step 3: Set up a Python virtual environment
python -m venv env

# Step 4: Activate it:
source env/bin/activate # on Linux-based systems
env\Scripts\activate # on Windows

# Step 5: Install the libraries required:
pip install -r requirements.txt

# Step 6: Launch the server by running the command below:
python app.py
```

### Frontend Setup:
Now in a separate terminal, do the following:
```sh
# Step 1: Navigate to the project directory.
cd <TCNA/Deployed_Server>

# Step 2: Install the necessary dependencies.
npm i

# Step 3: Start the development server with auto-reloading and an instant preview.
npm run dev
```

The website will now be available at [http://localhost:8081](http://localhost:8081).
