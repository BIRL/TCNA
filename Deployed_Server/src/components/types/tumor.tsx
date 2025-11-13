export interface TumorData {
  sample: string;
  cancer_type: string;
  depth2?: number;
  depth?: number;
}

export interface SampleInfo {
  barcode: string;
  cancerType: string;
}

export interface MetricData {
  sample_id: string;
  tpm: number | null;
  fpkm: number | null;
  fpkm_uq: number | null;
}