// // export interface GeneStats {
// //   gene: string;
// //   ensembl_id: string;
// //   mean_tumor: number;
// //   mean_normal: number;
// //   cv_tumor: number;
// //   cv_normal: number;
// //   logFC: number;
// // }

// // export interface Enrichment {
// //   Term: string;
// //   "P-value": number;
// //   "Adjusted P-value": number;
// //   "Combined Score": number;
// //   Genes: string[];
// // }

// // export interface ResultsData {
// //   enrichment: Enrichment[];
// //   top_genes: string[];
// //   gene_stats: GeneStats[];
// // }
// export interface GeneStats {
//   gene: string;
//   ensembl_id: string;
//   mean_tumor: number;
//   mean_normal: number;
//   cv_tumor: number;
//   cv_normal: number;
//   logFC: number;
// }

// export interface Enrichment {
//   Term: string;
//   "P-value": number;
//   "Adjusted P-value": number;
//   "Combined Score": number;
//   Genes: string[];
// }

// export interface HeatmapData {
//   [gene: string]: {
//     [key: string]: number; // e.g., "colon Tumor", "colon Normal", "cervix Tumor", "cervix Normal"
//   };
// }

// export interface ResultsData {
//   enrichment: Enrichment[];
//   top_genes: string[];
//   gene_stats: GeneStats[];
//   heatmap_data: HeatmapData; // Added to include the new heatmap data structure
// }
// export interface GeneStats {
//   gene: string;
//   ensembl_id: string;
//   metrics: {
//     [cancer: string]: {
//       cv_normal: number;
//       cv_tumor: number;
//       logfc: number;
//       mean_normal: number;
//       mean_tumor: number;
//     };
//   };
// }

// export interface Enrichment {
//   Term: string;
//   "P-value": number;
//   "Adjusted P-value": number;
//   "Combined Score": number;
//   Genes: string[];
//   GeneSet?: string; // Optional, as it’s included in the API response
// }

// export interface HeatmapData {
//   [gene: string]: {
//     [key: string]: number; // e.g., "colon Tumor", "colon Normal", "cervix Tumor", "cervix Normal"
//   };
// }

// export interface ResultsData {
//   enrichment: Enrichment[];
//   top_genes: string[];
//   gene_stats: GeneStats[];
//   heatmap_data: HeatmapData;
//   noise_metrics: { [key: string]: any }; // Optional, as it’s included but empty in the API response
//   warning?: string | null; // Optional, as it’s included in the API response
// }
export interface GeneStats {
  gene: string;
  ensembl_id: string;
  metrics: {
    [cancer: string]: {
      cv_normal: number;
      cv_tumor: number;
      logfc: number;
      mean_normal: number;
      mean_tumor: number;
    };
  };
}

export interface Enrichment {
  Term: string;
  "P-value": number;
  "Adjusted P-value": number;
  "Combined Score": number;
  Genes: string[];
  GeneSet?: string;
  Overlap: string;
  "Odds Ratio": number;
  "Term ID": string;
}

export interface HeatmapData {
  [gene: string]: {
    [key: string]: number;
  };
}

export interface ResultsData {
  enrichment: Enrichment[];
  top_genes: string[];
  gene_stats: GeneStats[];
  heatmap_data: HeatmapData;
  noise_metrics: { [key: string]: any };
  warning?: string | null;
}