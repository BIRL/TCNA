-- Run this against tcna_db (same database as the rest of TCNA)

USE tcna_db;

-- Reference table: one row per Census gene, from Cosmic_CancerGeneCensus
CREATE TABLE IF NOT EXISTS cosmic_genes (
    gene_symbol VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(255),
    tier INT,
    role_in_cancer VARCHAR(255),
    mutation_types VARCHAR(255),
    tumour_types_somatic TEXT
);

-- Aggregated mutation counts: one row per (gene, sample) pair
-- mutation_count = how many confirmed-somatic mutation records that sample has in that gene
CREATE TABLE IF NOT EXISTS cosmic_mutation_counts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    gene_symbol VARCHAR(50) NOT NULL,
    cosmic_sample_id VARCHAR(50) NOT NULL,
    sample_name VARCHAR(100),
    mutation_count INT NOT NULL,
    FOREIGN KEY (gene_symbol) REFERENCES cosmic_genes(gene_symbol)
);

CREATE INDEX idx_cosmic_mut_gene ON cosmic_mutation_counts (gene_symbol);
