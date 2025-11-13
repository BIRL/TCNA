
// export interface GeneStats {
//   gene: string;
//   site: string;
//   ensembl_id: string;
//   gene_symbol: string;
//   cv_tumor?: number; // Optional to handle cases where tumor data is not selected
//   mean_tumor?: number;
//   std_tumor?: number;
//   mad_tumor?: number;
//   cv_squared_tumor?: number;
//   cv_normal?: number; // Optional to handle cases where normal data is not selected
//   mean_normal?: number;
//   std_normal?: number;
//   mad_normal?: number;
//   cv_squared_normal?: number;
//   tumorSamples: number;
//   normalSamples: number;
//   logfc?: number; // Optional as it requires both tumor and normal groups
//   logfcMessage?: string; // Optional for cases where logfc is not calculated
//   tumorValues?: number[]; // Optional for cases with no tumor data
//   normalValues?: number[]; // Optional for cases with no normal data
//   // Dynamic keys for different normalization methods (tpm, fpkm, fpkm_uq)
//   [key: `${string}_${'tumor' | 'normal'}_${'tpm' | 'fpkm' | 'fpkm_uq'}`]: number | undefined;
//   // warning?; string[];
// }

// export interface ResultsData {
//   resultsData: GeneStats[];
//   totalTumorSamples: number;
//   totalNormalSamples: number;
// }
export interface GeneStats {
  gene: string;
  site: string;
  ensembl_id: string;
  gene_symbol: string;
  cv_tumor?: number; // Optional to handle cases where tumor data is not selected
  mean_tumor?: number;
  std_tumor?: number;
  mad_tumor?: number;
  cv2_tumor?: number;
  cv_normal?: number; // Optional to handle cases where normal data is not selected
  mean_normal?: number;
  std_normal?: number;
  mad_normal?: number;
  cv2_normal?: number;
  tumorSamples: number;
  normalSamples: number;
  logfc?: number; // Optional as it requires both tumor and normal groups
  logfcMessage?: string; // Optional for cases where logfc is not calculated
  tumorValues?: number[]; // Optional for cases with no tumor data
  normalValues?: number[]; // Optional for cases with no normal data
  normalizationMethod: string; // e.g., 'tpm', 'fpkm', 'fpkm_uq'
  dataFormat: 'raw' | 'log2'; // Indicates whether data is raw or log2-transformed
  warning?: string; // Optional warning message, corrected from string[] to string
  // Dynamic keys for different normalization methods and data formats
  [key: `${string}_${'tumor' | 'normal'}_${'tpm' | 'fpkm' | 'fpkm_uq'}`]: number | undefined;
}

export interface ResultsData {
  resultsData: GeneStats[];
  totalTumorSamples: number;
  totalNormalSamples: number;
  // siteSampleCounts: { site: string; tumor: number; normal: number }[];
  // availableSites: { id: number; name: string }[];
}