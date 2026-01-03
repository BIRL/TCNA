CREATE DATABASE IF NOT EXISTS tcna_db;
USE tcna_db;
-- Create genes table
CREATE TABLE IF NOT EXISTS genes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ensembl_id VARCHAR(50) UNIQUE NOT NULL,
    gene_symbol VARCHAR(100)
);

-- Create sites table
CREATE TABLE IF NOT EXISTS sites (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

-- Create cancer_types table
CREATE TABLE IF NOT EXISTS cancer_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tcga_code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL DEFAULT '',  
    site_id INT,
    FOREIGN KEY (site_id) REFERENCES sites(id)
);

-- Create samples table
CREATE TABLE IF NOT EXISTS samples (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_barcode VARCHAR(100) UNIQUE NOT NULL,
    sample_type VARCHAR(20),
    cancer_type_id INT,
    FOREIGN KEY (cancer_type_id) REFERENCES cancer_types(id)
);

-- Create gene_expressions table
CREATE TABLE IF NOT EXISTS gene_expressions (
    gene_id INT NOT NULL,
    sample_id INT NOT NULL,
    tpm DOUBLE,
    fpkm DOUBLE,
    fpkm_uq DOUBLE,
    PRIMARY KEY (gene_id, sample_id),
    FOREIGN KEY (gene_id) REFERENCES genes(id),
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- Create depth2_scores table
CREATE TABLE IF NOT EXISTS depth2_scores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id INT UNIQUE NOT NULL,
    tpm DOUBLE,
    fpkm DOUBLE,
    fpkm_uq DOUBLE,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- Create depth_scores table
CREATE TABLE IF NOT EXISTS depth_scores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id INT UNIQUE NOT NULL,
    tpm DOUBLE,
    fpkm DOUBLE,
    fpkm_uq DOUBLE,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);