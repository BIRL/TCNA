-- indexes
ALTER TABLE gene_expressions 
    ADD INDEX idx_gene_sample (gene_id, sample_id),
    ADD INDEX idx_sample_gene (sample_id, gene_id);

ALTER TABLE samples 
    ADD INDEX idx_samples_type_cancer (cancer_type_id, sample_type);

ALTER TABLE metadata 
    ADD INDEX idx_metadata_sample (sample_id);

ALTER TABLE genes 
    ADD INDEX idx_genes_symbol (id, gene_symbol);

-- Create a View That Joins Gene Expressions → Samples → Cancer Types
CREATE OR REPLACE VIEW gene_expr_with_meta AS
SELECT
    ge.gene_id,
    ge.sample_id,
    ge.tpm,
    ge.fpkm,
    ge.fpkm_uq,
    s.sample_type,
    ct.id AS cancer_type_id,
    ct.tcga_code
FROM gene_expressions ge
JOIN samples s ON s.id = ge.sample_id
JOIN cancer_types ct ON ct.id = s.cancer_type_id;


-- Build a Ranked Noisy-Gene Query (Window Function)
-- WITH stats AS (
--     SELECT
--         cancer_type_id,
--         tcga_code,
--         sample_type,
--         gene_id,
--         AVG(tpm)        AS mean_expr,
--         STDDEV_SAMP(tpm) AS sd_expr,
--         STDDEV_SAMP(tpm) / NULLIF(AVG(tpm), 0) AS cv
--     FROM gene_expr_with_meta
--     WHERE tpm > 0
--     GROUP BY cancer_type_id, tcga_code, sample_type, gene_id
-- ),
-- ranked AS (
--     SELECT
--         s.*,
--         ROW_NUMBER() OVER (
--             PARTITION BY cancer_type_id, sample_type
--             ORDER BY cv DESC
--         ) AS rn
--     FROM stats s
-- )
-- SELECT *
-- FROM ranked
-- WHERE rn <= 50;
