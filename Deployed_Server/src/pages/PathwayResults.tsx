import React, { useMemo, useCallback, useReducer, useEffect, useRef, useState } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Download, Activity, Users, Info } from "lucide-react";
import { PlotlyHeatmap } from "@/components/charts/index";
import Header from "@/components/header";
import Footer from "@/components/footer";
import { useCache } from "@/hooks/use-cache";
import { LoadingSpinner } from "@/components/ui/loadingSpinner";
import FilterPanel from "@/components/FilterPanel";
import { DataTable } from "@/components/ui/data-table";
import CollapsibleCard from "@/components/ui/collapsible-card";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription } from "@/components/ui/alert";
import SampleCounts from "@/components/SampleCounts";

// ====================== INTERFACES ======================
export interface GeneNoise {
  mean_tumor: number | null;
  mean_normal: number | null;
  cv_tumor: number | null;
  cv_normal: number | null;
  logfc: number | null;
}

export interface GeneNoiseData {
  [norm: string]: {
    [gene: string]: {
      [site: string]: GeneNoise;
    };
  };
}

export interface Enrichment {
  Term: string;
  Database: string;
  FDR: number;
  MatchingGenes: string[];
  Description?: string;
  GeneCount?: number;
  EnrichmentScore?: number;
}

export interface EnrichmentResults {
  enrichment: Enrichment[];
  warning?: string | null;
}

export interface ResultsData {
  raw: EnrichmentResults;
  log2: EnrichmentResults;
}

interface FilterOption {
  id: string;
  label: string;
  tooltip?: string;
}

interface FilterSection {
  title: string;
  id: string;
  options: FilterOption[];
  isMasterCheckbox?: boolean;
  type: "checkbox" | "radio";
  defaultOpen?: boolean;
  customRender?: () => React.ReactNode;
}

interface FilterState {
  selectedGroups: string[];
  selectedSites: string[];
  selectedGenes: string[];
  normalizationMethod: string;
  dataFormats: { mean: "raw" | "log2"; cv: "raw" | "log2"; logfc: "log2" };
  selectedPathway: Enrichment | null;
  sortBy: "fdr";
  sortOrder: "asc" | "desc";
  logFCThreshold: number;
  isSampleCountsOpen: boolean;
  analysisMode: "enrichment" | "neighbors";
  geneInput: string;
  showNetwork: boolean;
}

const initialFilterState: FilterState = {
  selectedGroups: ["normal", "tumor"],
  selectedSites: [],
  selectedGenes: [],
  normalizationMethod: "tpm",
  dataFormats: { mean: "raw", cv: "raw", logfc: "log2" },
  selectedPathway: null,
  sortBy: "fdr",
  sortOrder: "asc",
  logFCThreshold: 0.7,
  isSampleCountsOpen: true,
  analysisMode: "enrichment",
  geneInput: "",
  showNetwork: false,
};

// ====================== REDUCER ======================
type FilterAction =
  | { type: "SET_GROUPS"; payload: string[] }
  | { type: "SET_SITES"; payload: string[] }
  | { type: "SET_GENES"; payload: string[] }
  | { type: "SET_NORMALIZATION"; payload: string }
  | { type: "SET_DATA_FORMAT"; payload: { metric: keyof FilterState["dataFormats"]; format: "raw" | "log2" } }
  | { type: "SET_SELECTED_PATHWAY"; payload: Enrichment | null }
  | { type: "SET_SORT_BY"; payload: "fdr" }
  | { type: "SET_SORT_ORDER"; payload: "asc" | "desc" }
  | { type: "SET_LOGFC_THRESHOLD"; payload: number }
  | { type: "TOGGLE_SAMPLE_COUNTS" }
  | { type: "SET_ANALYSIS_MODE"; payload: "enrichment" | "neighbors" }
  | { type: "SET_GENE_INPUT"; payload: string }
  | { type: "TOGGLE_NETWORK" };

const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
  switch (action.type) {
    case "SET_GROUPS": return { ...state, selectedGroups: action.payload };
    case "SET_SITES": return { ...state, selectedSites: action.payload };
    case "SET_GENES": return { ...state, selectedGenes: action.payload };
    case "SET_NORMALIZATION": return { ...state, normalizationMethod: action.payload };
    case "SET_DATA_FORMAT":
      if (action.payload.metric === "logfc") return state;
      return { ...state, dataFormats: { ...state.dataFormats, [action.payload.metric]: action.payload.format } };
    case "SET_SELECTED_PATHWAY": return { ...state, selectedPathway: action.payload };
    case "SET_SORT_BY": return { ...state, sortBy: action.payload };
    case "SET_SORT_ORDER": return { ...state, sortOrder: action.payload };
    case "SET_LOGFC_THRESHOLD": return { ...state, logFCThreshold: action.payload };
    case "TOGGLE_SAMPLE_COUNTS": return { ...state, isSampleCountsOpen: !state.isSampleCountsOpen };
    case "SET_ANALYSIS_MODE": return { ...state, analysisMode: action.payload };
    case "SET_GENE_INPUT": return { ...state, geneInput: action.payload };
    case "TOGGLE_NETWORK": return { ...state, showNetwork: !state.showNetwork };
    default: return state;
  }
};

// ====================== ENRICHMENT HOOK WITH CACHING ======================
const useEnrichmentData = (
  params: any,
  filterState: FilterState,
  areGenesSubsetOfInitial: (genes: string[]) => boolean
) => {
  const [state, setState] = useState<{
    resultsData: ResultsData;
    isLoading: boolean;
    error: string | null;
    totalTumorSamples: number;
    totalNormalSamples: number;
    siteSampleCounts: { site: string; tumor: number; normal: number }[];
    availableSites: { id: number; name: string }[];
    fetchedSites: string[];
  }>({
    resultsData: { raw: { enrichment: [] }, log2: { enrichment: [] } },
    isLoading: false,
    error: null,
    totalTumorSamples: 0,
    totalNormalSamples: 0,
    siteSampleCounts: [],
    availableSites: [],
    fetchedSites: [],
  });

  const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  const fetchEnrichment = useCallback(async (sites: string[]) => {
    if (!sites.length) {
      setState((s) => ({ ...s, error: "Select at least one site", isLoading: false }));
      return;
    }

    const cacheKey = generateCacheKey({
      endpoint: "enrichment",
      sites,
      genes: filterState.selectedGenes,
      pathway: filterState.selectedPathway?.Term || params.pathwayId,
      library: params.library,
      mode: filterState.analysisMode,
    });

    const cached = getCachedData(cacheKey);
    if (cached) {
      setState((s) => ({ ...s, ...cached, isLoading: false, error: null }));
      return;
    }

    setState((s) => ({ ...s, isLoading: true }));
    try {
      // Build the JSON body instead of query params
      const body = {
        cancer: sites.join(","), 
        top_n: 1000,
        mode: filterState.analysisMode,
        genes: filterState.selectedGenes,
        pathway: filterState.selectedPathway?.Term || null,
      };

      const res = await fetch(`/api/pathway-analysis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();

      const payload = {
        resultsData: { raw: data.raw, log2: data.log2 } as ResultsData,
        totalTumorSamples: Object.values(data.sample_counts).reduce(
          (sum: any, c: any) => sum + (c.tumor ?? 0),
          0
        ) as number,
        totalNormalSamples: Object.values(data.sample_counts).reduce(
          (sum: any, c: any) => sum + (c.normal ?? 0),
          0
        ) as number,
        siteSampleCounts: Object.entries(data.sample_counts).map(
          ([site, c]: [string, any]) => ({
            site,
            tumor: c.tumor ?? 0,
            normal: c.normal ?? 0,
          })
        ) as { site: string; tumor: number; normal: number }[],
        availableSites: data.available_sites as { id: number; name: string }[],
        fetchedSites: sites,
      };

      setCachedData(cacheKey, payload);
      setState((s) => ({ ...s, ...payload, isLoading: false, error: null }));
    } catch (e: any) {
      setState((s) => ({ ...s, error: e.message, isLoading: false }));
    }

  }, [
    filterState.selectedGenes,
    filterState.selectedSites,
    filterState.analysisMode,
    filterState.selectedPathway,
    params.library,
    params.pathwayId,
    getCachedData,
    setCachedData,
    generateCacheKey,
  ]);

  const debounced = useCallback((sites: string[]) => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    timeoutRef.current = setTimeout(() => fetchEnrichment(sites), 500);
  }, [fetchEnrichment]);

  useEffect(() => {
    if (filterState.selectedSites.length) {
      debounced(filterState.selectedSites);
    }
  }, [filterState.selectedSites, debounced]);

  return { ...state, refetchEnrichment: debounced };
};

// ====================== GENE NOISE HOOK ======================
const useGeneNoise = (
  sites: string[],
  genes: string[],
  normalizationMethod: string,
  cancerTypes?: string[]
) => {
  const [state, setState] = useState<{
    data: GeneNoiseData;
    isLoading: boolean;
    error: string | null;
  }>({
    data: {},
    isLoading: false,
    error: null,
  });

  const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
  const abortRef = useRef<AbortController | null>(null);

  const fetchNoise = useCallback(async () => {
  if (!sites.length || !genes.length) return;

  const cacheKey = generateCacheKey({ endpoint: "gene-noise", sites, genes });
  const cached = getCachedData(cacheKey);
  if (cached) {
    setState({ data: cached, isLoading: false, error: null });
    return;
  }

  if (abortRef.current) abortRef.current.abort();
  const ctrl = new AbortController();
  abortRef.current = ctrl;

  setState(s => ({ ...s, isLoading: true }));
  try {
    const body = {
      cancer: sites,
      genes: genes,
      ...(cancerTypes && cancerTypes.length && { cancer_types: cancerTypes }),
    };

    const res = await fetch("/api/gene-noise-pathway", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }

    const json = await res.json();
    const payload = json.gene_noise;
    setCachedData(cacheKey, payload);
    setState({ data: payload, isLoading: false, error: null });
  } catch (e: any) {
    if (e.name !== "AbortError") {
      console.error("[useGeneNoise] error:", e);
      setState(s => ({ ...s, isLoading: false, error: e.message }));
    }
  }
}, [sites, genes, cancerTypes, getCachedData, setCachedData, generateCacheKey]);

  const debounced = useMemo(() => {
    let t: NodeJS.Timeout;
    return () => { clearTimeout(t); t = setTimeout(fetchNoise, 300); };
  }, [fetchNoise]);

  return { ...state, refetchNoise: debounced };
};

// ====================== MAIN COMPONENT ======================
const PathwayResults: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { getCachedData, generateCacheKey } = useCache<any>();

  const params = useMemo(() => ({
    sites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
    cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
    library: searchParams.get("library") || "KEGG_2021_Human",
    genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
    topN: parseInt(searchParams.get("top_n") || "1000", 10),
    mode: searchParams.get("mode") || "enrichment",
    pathwayId: searchParams.get("pathway"),
  }), [searchParams]);

  const initialGenesRef = useRef<string[]>(params.genes);

  useEffect(() => { window.scrollTo(0, 0); }, []);

  // const [filterState, dispatch] = useReducer(filterReducer, {
  //   ...initialFilterState,
  //   selectedSites: params.sites,
  //   selectedGenes: params.genes,
  //   analysisMode: params.mode as "enrichment" | "neighbors",
  //   geneInput: params.genes.join(", "),
  //   selectedPathway: null,
  // });

  const initialPathway: Enrichment | null = useMemo(() => {
  if (!params.pathwayId) return null;
  const genesStr = searchParams.get("pathwayGenes");
  return {
    Term: params.pathwayId,
    Database: searchParams.get("pathwayCategory") || "Unknown",
    FDR: 0,
    MatchingGenes: genesStr ? genesStr.split("|").filter(Boolean) : [],
    Description: searchParams.get("pathwayDescription") || searchParams.get("pathwayLabel") || params.pathwayId,
  };
}, [params.pathwayId, searchParams]);

  const [filterState, dispatch] = useReducer(filterReducer, {
    ...initialFilterState,
    selectedSites: params.sites,
    selectedGenes: params.genes,
    analysisMode: params.mode as "enrichment" | "neighbors",
    geneInput: params.genes.join(", "),
    selectedPathway: initialPathway,
  });


  // Helper: are current genes a subset of the initial URL genes?
  const areGenesSubsetOfInitial = useCallback(
    (genes: string[]) => {
      if (!initialGenesRef.current.length) return false;
      return genes.every(g => initialGenesRef.current.includes(g));
    },
    []
  );

  // === ENRICHMENT DATA ===
  const {
    resultsData,
    isLoading,
    error: enrichError,
    totalTumorSamples,
    totalNormalSamples,
    siteSampleCounts,
    availableSites,
    refetchEnrichment,
  } = useEnrichmentData(params, filterState, areGenesSubsetOfInitial);

  // === GENE NOISE DATA ===
  const selectedGenesForNoise = filterState.selectedPathway?.MatchingGenes ?? filterState.selectedGenes;

  const { data: noiseData, isLoading: noiseLoading, refetchNoise } = useGeneNoise(
    filterState.selectedSites,
    selectedGenesForNoise,
    filterState.normalizationMethod,
    params.cancerTypes
  );


  const prevGenes = useRef<string[]>();
  const prevSites = useRef<string[]>();

  useEffect(() => {
    const sitesChanged =
      !prevSites.current ||
      JSON.stringify(prevSites.current.sort()) !==
        JSON.stringify(filterState.selectedSites.sort());

    const genesChanged =
      !prevGenes.current ||
      JSON.stringify(prevGenes.current.sort()) !==
        JSON.stringify(selectedGenesForNoise.sort());

    const needFetch =
      sitesChanged ||
      (genesChanged && !areGenesSubsetOfInitial(selectedGenesForNoise));

    if (needFetch) {
      prevSites.current = filterState.selectedSites;
      prevGenes.current = selectedGenesForNoise;
      refetchNoise();
    }
  }, [
    filterState.selectedSites,
    selectedGenesForNoise,
    areGenesSubsetOfInitial,
    refetchNoise,
  ]);

  // === UPDATE FILTERS ===
  const updateFilters = useCallback((updates: Partial<FilterState>) => {
    if (updates.selectedSites !== undefined) dispatch({ type: "SET_SITES", payload: updates.selectedSites });
    if (updates.selectedGenes !== undefined) dispatch({ type: "SET_GENES", payload: updates.selectedGenes });
    if (updates.selectedPathway !== undefined) dispatch({ type: "SET_SELECTED_PATHWAY", payload: updates.selectedPathway });
    if (updates.normalizationMethod !== undefined) dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
    if (updates.dataFormats) {
      Object.entries(updates.dataFormats).forEach(([metric, format]) => {
        if (metric !== "logfc") {
          dispatch({ type: "SET_DATA_FORMAT", payload: { metric: metric as keyof FilterState["dataFormats"], format: format as "raw" | "log2" } });
        }
      });
    }

    const newParams: any = { ...Object.fromEntries(searchParams) };
    if (updates.selectedSites !== undefined) {
      if (updates.selectedSites && updates.selectedSites.length) newParams.sites = updates.selectedSites.join(",");
      else delete newParams.sites;
    }
    if (updates.selectedGenes !== undefined) {
      if (updates.selectedGenes && updates.selectedGenes.length) newParams.genes = updates.selectedGenes.join(",");
      else delete newParams.genes;
    }
    if (updates.selectedPathway !== undefined) {
      if (updates.selectedPathway) newParams.pathway = updates.selectedPathway.Term;
      else delete newParams.pathway;
    }
    setSearchParams(newParams);
  }, [searchParams, setSearchParams]);

  const handleGeneInputSubmit = useCallback(() => {
    const genes = filterState.geneInput
      .split(",")
      .map((g) => g.trim().toUpperCase())
      .filter((g) => g.length > 0);

    updateFilters({ selectedGenes: genes, geneInput: genes.join(", "), selectedPathway: null });
  }, [filterState.geneInput, updateFilters]);

  const customFilters: FilterSection[] = useMemo(() => {
    const availableGenes = filterState.selectedPathway
      ? filterState.selectedPathway.MatchingGenes.map((gene) => ({ id: gene, label: gene }))
      : [
          ...new Set(
            ["raw", "log2"].flatMap((scale) =>
              ["tpm", "fpkm", "fpkm_uq"].flatMap((method) => Object.keys(resultsData[scale]?.[method]?.gene_stats || {}))
            )
          ),
        ].map((gene) => ({ id: gene, label: gene }));

    return [
      {
        title: "Sites",
        id: "sites",
        type: "checkbox",
        options: availableSites.map((site) => ({
          id: site.name,
          label: site.name,
          tooltip: `Cancer site: ${site.name}`,
        })),
        isMasterCheckbox: true,
        defaultOpen: false,
      },
      {
        title: "LogFC Threshold",
        id: "logfc-threshold",
        type: "checkbox",
        options: [],
        defaultOpen: true,
        customRender: () => (
          <div className="flex items-center space-x-3 p-3 bg-gray-50 rounded-md">
            <Label htmlFor="logfc-input" className="text-sm font-medium text-blue-900 whitespace-nowrap">
              |Log₂FC| ≥
            </Label>
            <Input
              id="logfc-input"
              type="number"
              min="0"
              step="0.1"
              value={filterState.logFCThreshold}
              onChange={(e) => {
                const raw = e.target.value;
                const val = parseFloat(e.target.value);
                if (raw === "") return;
                if (!isNaN(val) && val >= 0) {
                  dispatch({ type: "SET_LOGFC_THRESHOLD", payload: val });
                }
              }}
              className="w-20 text-sm"
            />
            <span className="text-xs text-gray-600">(≥ 3 genes per cancer)</span>
          </div>
        ),
      },
    ];
  }, [availableSites, resultsData, params.genes, filterState.selectedPathway, filterState.logFCThreshold]);

  // === PATHWAY TABLE ===
  const currentResults = resultsData[filterState.dataFormats.mean]?.[filterState.normalizationMethod] || { enrichment: [], gene_stats: {} } as any;
  const currentEnrichment = currentResults.enrichment || [];
  const enrichedPathways = useMemo(() => {
    return (currentEnrichment as any[]).map((p: any, i: number) => {
      const originalMatchingGenes: string[] = p.MatchingGenes ?? p.matchingGenes ?? p.inputGenes ?? p.inputGenesList ?? p.genes ?? [];
      const Term: string = p.Term ?? p.term ?? p.value ?? "";
      const FDR: number = p.FDR ?? p.fdr ?? p.fdr_value ?? 1;
      const Database: string = p.Database ?? p.database ?? p.source ?? "";
      const MatchingGenes: string[] = originalMatchingGenes;
      const Description: string = p.Description ?? p.description ?? "";
      const GeneCount: number = p.GeneCount ?? p.gene_count ?? (Array.isArray(MatchingGenes) ? MatchingGenes.length : 0);
      const EnrichmentScore: number = p.EnrichmentScore ?? p.score ?? 0;

      return {
        Term,
        Database,
        FDR,
        MatchingGenes,
        Description,
        GeneCount,
        EnrichmentScore,
        Rank: i + 1,
      } as Enrichment & { Rank: number };
    }).sort((a, b) => a.FDR - b.FDR);
  }, [currentEnrichment]);

  const formatFDR = (fdr: number) => fdr.toExponential(2);

  // === RENDER HELPERS ===
  // const renderValue = (val: number | null | undefined) =>
  //   val == null || Number.isNaN(Number(val)) ? "—" : Number(val).toFixed(3);
  const renderValue = (val: number | null | undefined) =>
    val == null || Number.isNaN(val) ? 0.00 : Number(val).toFixed(3);

  const norm = filterState.normalizationMethod;
  const meanSource = resultsData[filterState.dataFormats.mean]?.[norm]?.gene_stats || {};
  const cvSource = resultsData[filterState.dataFormats.cv]?.[norm]?.gene_stats || {};
  const logfcSource = resultsData.log2?.[norm]?.gene_stats || {};

  const meanTableData = useMemo(() => {
    return Object.entries(meanSource).map(([gene, sites]) => ({
      gene,
      ...Object.fromEntries(
        filterState.selectedSites.flatMap(site => {
          const s = (sites as any)[site.toLowerCase()] || {};
          return [
            [`${site}_normal`, s.mean_normal ?? null],
            [`${site}_tumor`, s.mean_tumor ?? null],
          ];
        })
      ),
    }));
  }, [meanSource, filterState.selectedSites, filterState.dataFormats.mean, norm, resultsData]);

  const logfcTableData = useMemo(() => {
    return Object.entries(logfcSource).map(([gene, sites]) => ({
      gene,
      ...Object.fromEntries(
        filterState.selectedSites.map(site => {
          const siteKey = site.toLowerCase();
          const siteStats = (sites as any)[siteKey] || {};
          return [site, siteStats.logfc ?? null];
        })
      ),
    }));
  }, [logfcSource, filterState.selectedSites, norm]);

  const isNoiseFullyLoaded = useMemo(() => {
    if (!logfcSource || !filterState.selectedSites.length) return false;
    return filterState.selectedSites.every(site => {
      const lowerSite = site.toLowerCase();
      return Object.values(logfcSource).some((stats: any) => {
        const siteData = stats[lowerSite];
        return siteData && siteData.logfc != null;
      });
    });
  }, [logfcSource, filterState.selectedSites]);

  const cvTableData = useMemo(() => {
    return Object.entries(cvSource).map(([gene, sites]) => ({
      gene,
      ...Object.fromEntries(
        filterState.selectedSites.flatMap(site => {
          const s = (sites as any)[site.toLowerCase()] || {};
          return [
            [`${site}_normal`, s.cv_normal ?? null],
            [`${site}_tumor`, s.cv_tumor ?? null],
          ];
        })
      ),
    }));
  }, [cvSource, filterState.selectedSites, filterState.dataFormats.cv, norm, resultsData]);

  // === DOWNLOAD ===
  const downloadData = useCallback(
    (type: "enrichment" | "mean_expression" | "noise_metrics") => {
      const selectedSites = filterState.selectedSites as string[];
      let data: any[] = [];
      let headers: string[] = [];
      let filename = `pathway_analysis_${selectedSites.join("_")}_${filterState.dataFormats.mean}_${filterState.normalizationMethod}`;
      if (type === "enrichment") {
        filename = `enrichment_results_${filterState.normalizationMethod}.csv`;
        headers = ["Rank", "Pathway/GO Term", "Database", "FDR", "Matching Genes", "Description", "Gene Count"];
        const rows = enrichedPathways.map((row) =>
          [
            row.Rank,
            row.Term,
            row.Database,
            formatFDR(row.FDR),
            row.MatchingGenes.join(" || "),
            row.Description || "N/A",
            row.GeneCount || 0,
          ].join(",")
        );
        const content = [headers.join(","), ...rows].join("\n");
        const blob = new Blob([content], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
      } else if (type === "mean_expression") {
        data = Object.entries(currentResults.gene_stats);
        headers = ["Gene", ...selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])];
        filename = `mean_expression_${filename}.csv`;
        const rows = data.map(([gene, stats]) => {
          const metrics = selectedSites
            .map((site) => {
              const lowerSite = site.toLowerCase();
              const metric = stats[lowerSite] || {};
              return [metric.mean_normal?.toFixed(2) || "0.00", metric.mean_tumor?.toFixed(2) || "0.00"];
            })
            .flat();
          return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
        });
        const content = [headers.join(","), ...rows].join("\n");
        const blob = new Blob([content], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
      } else if (type === "noise_metrics") {
        data = Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {});
        headers = ["Gene", ...selectedSites.flatMap((site) => [`${site} Normal CV`, `${site} Tumor CV`, `${site} Log2FC`])];
        filename = `noise_metrics_${filename}.csv`;
        const rows = data.map(([gene, stats]) => {
          const ensembl_id = stats.ensembl_id || "N/A";
          const metrics = selectedSites
          
            .map((site) => {
              const lowerSite = site.toLowerCase();
              const metric = stats[lowerSite] || {};
              return [
                metric.cv_normal?.toFixed(2) || "0.00",
                metric.cv_tumor?.toFixed(2) || "0.00",
                metric.logfc?.toFixed(2) || "0.00",
              ];
            })
            .flat();
          return [gene, ensembl_id, ...metrics].join(","); 
        });
        const content = [headers.join(","), ...rows].join("\n");
        const blob = new Blob([content], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
      }
    },
    [enrichedPathways, filterState.normalizationMethod, resultsData, filterState.dataFormats, filterState.selectedSites]
  );

  // === Z-SCORES ===
  const getZValues = useCallback((key: "mean" | "cv" | "logfc") => {
    const source =
      key === "mean"
        ? resultsData[filterState.dataFormats.mean][norm]?.gene_stats
        : key === "cv"
        ? resultsData[filterState.dataFormats.cv][norm]?.gene_stats
        : resultsData.log2[filterState.normalizationMethod]?.gene_stats;

    if (!source) return [];

    const values: number[] = [];
    Object.entries(source).forEach(([_, sites]) => {
      filterState.selectedSites.forEach(site => {
        const s = (sites as any)[site.toLowerCase()] || {};
        if (key === "mean") {
          if (s.mean_normal != null) values.push(s.mean_normal);
          if (s.mean_tumor != null) values.push(s.mean_tumor);
        } else if (key === "cv") {
          if (s.cv_normal != null) values.push(s.cv_normal);
          if (s.cv_tumor != null) values.push(s.cv_tumor);
        } else if (key === "logfc" && s.logfc != null) {
          values.push(s.logfc);
        }
      });
    });

    const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    const std = values.length > 1
      ? Math.sqrt(values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (values.length - 1))
      : 0;

    return Object.keys(source).map(gene => {
      const sites = source[gene] || {};
      return filterState.selectedSites.flatMap(site => {
        const s = sites[site.toLowerCase()] || {};
        const z = (v: number | null) => v == null ? null : std > 0 ? (v - mean) / std : 0;
        if (key === "logfc") return [s.logfc != null ? z(s.logfc) : null];
        return [z(s.mean_normal), z(s.mean_tumor)];
      });
    });
  }, [
    resultsData,
    filterState.dataFormats,
    filterState.selectedSites,
    filterState.normalizationMethod,
    norm,
  ]);

  // === WARNING: NO NORMAL SAMPLES ===
  const hasNoNormal = filterState.selectedSites.some(site => {
    const c = siteSampleCounts.find(s => s.site === site);
    return c && c.normal === 0;
  });

  
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="grid grid-cols-[320px_1fr] gap-6">
            <FilterPanel
              normalizationMethod={filterState.normalizationMethod}
              setNormalizationMethod={(value) => updateFilters({ normalizationMethod: value })}
              customFilters={customFilters}
              onFilterChange={(filterId, value) => {
                if (filterId === "sites") {
                  updateFilters({ selectedSites: value });
                } else if (filterId === "genes") {
                  updateFilters({ selectedGenes: value });
                }
              }}
              selectedValues={{
                sites: filterState.selectedSites,
                genes: filterState.selectedGenes,
              }}
            />

            <div className="flex-1">
              {isLoading ? (
                <LoadingSpinner message="Please wait..." />
              ) : enrichError ? (
                <div className="text-red-600">{enrichError}</div>
              ) : filterState.selectedSites.length === 0 ? (
                <div className="text-red-600">Please select at least one site.</div>
              ) : (
                <>
                  <Link
                    to="/pathway-analysis"
                    className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
                  >
                    <ArrowLeft className="h-4 w-4 mr-2" />
                    Back to Pathway Analysis
                  </Link>
                  <div className="mb-8">
                    <div className="flex items-center justify-between mb-4">
                      <h2 className="text-4xl font-bold text-blue-900">Results For Pathway Analysis</h2>
                    </div>
                    <div className="overflow-x-auto mb-6">
                      <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
                        <tbody>
                          <tr className="border-b">
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Normalization</th>
                            <td className="py-3 px-4 text-blue-700">{filterState.normalizationMethod.toUpperCase()}</td>
                          </tr>
                          <tr className="border-b">
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Selected Pathway</th>
                            <td className="py-3 px-4 text-blue-700">
                              {(() => {
                                const parts = [
                                  searchParams.get("pathwayDescription"),
                                  searchParams.get("pathwayLabel"),
                                  searchParams.get("pathwayId"),
                                ]
                                  .filter(Boolean) // remove null or empty values
                                  .map((val) => decodeURIComponent(val!));

                                return parts.length > 0 ? parts.join(" — ") : "None"; // use em dash for readability
                              })()}
                            </td>
                          </tr>
                          <tr>
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Cancer Site(s)</th>
                            <td className="py-3 px-4 text-blue-700">
                              {filterState.selectedSites.join(", ")}
                              {params.cancerTypes.length > 0 && ` (${params.cancerTypes.join(", ")})`}
                            </td>
                          </tr>
                        </tbody>
                      </table>
                      {hasNoNormal && (
                        <Alert className="mb-4">
                          <AlertDescription>Warning: Some sites have no normal samples. CV-normal and logFC will show 0.</AlertDescription>
                        </Alert>
                      )}
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                    <Card className="border-0 shadow-lg">
                      <CardContent className="flex flex-col items-center p-4 text-center">
                        <Users className="h-6 w-6 text-green-600 mb-2" />
                        <div className="text-2xl font-bold text-green-600">{totalNormalSamples}</div>
                        <div className="text-xs text-gray-600">Total Normal Samples</div>
                      </CardContent>
                    </Card>
                    <Card className="border-0 shadow-lg">
                      <CardContent className="flex flex-col items-center p-4 text-center">
                        <Users className="h-6 w-6 text-red-600 mb-2" />
                        <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
                        <div className="text-xs text-gray-600">Total Tumor Samples</div>
                      </CardContent>
                    </Card>
                  </div>

                  <SampleCounts
                    isOpen={filterState.isSampleCountsOpen}
                    toggleOpen={() => dispatch({ type: "TOGGLE_SAMPLE_COUNTS" })}
                    siteSampleCounts={siteSampleCounts}
                    selectedSites={filterState.selectedSites}
                    selectedGroups={filterState.selectedGroups}
                  />

                  {currentResults.length === 0 ? (
                    <Card className="shadow-lg p-6 text-center text-blue-700">
                      <Activity className="w-10 h-10 mx-auto mb-3" />
                      <p className="text-lg">No pathway enrichment results found for the selected parameters.</p>
                    </Card>
                  ) : (
                    <>
                      <CollapsibleCard title="Mean Expression" className="mb-4">
                        <PlotlyHeatmap
                          title="Mean Expression (Z-scores)"
                          data={Object.entries(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
                            .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)
                            .map(([, stats]) => stats)}
                          xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
                          yValues={Object.keys(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
                            .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)}
                          zValues={getZValues("mean")}
                          zLabel={`Mean Expression (${filterState.dataFormats.mean.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
                          chartKey="expression-heatmap"
                          xLabel="Sample Types"
                          yLabel="Genes"
                        />
                      </CollapsibleCard>

                      <CollapsibleCard title="Coefficient of Variation (CV)" className="mb-4">
                        <PlotlyHeatmap
                          title="Coefficient of Variation (Z-scores)"
                          data={Object.entries(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
                            .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)
                            .map(([, stats]) => stats)}
                          xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
                          yValues={Object.keys(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
                            .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)}
                          zValues={getZValues("cv")}
                          zLabel={`Noise (${filterState.dataFormats.cv.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
                          chartKey="cv-heatmap"
                          xLabel="Sample Types"
                          yLabel="Genes"
                        />
                      </CollapsibleCard>

                      <CollapsibleCard title="Differential Noise" className="mb-4">
                        <PlotlyHeatmap
                          title={`Log₂(CV<sub>tumor</sub> / CV<sub>normal</sub>)`}
                          data={Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
                            .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)
                            .map(([, stats]) => stats)}
                          xValues={filterState.selectedSites}
                          yValues={Object.keys(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
                            .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene) ?? true)}
                          zValues={getZValues("logfc")}
                          zLabel="Log₂FC"
                          chartKey="logfc-heatmap"
                          xLabel="Cancer Sites"
                          yLabel="Genes"
                        />
                      </CollapsibleCard>

                      <CollapsibleCard
                        title="Mean Expression"
                        className="mb-4"
                        downloadButton={
                          <Button onClick={() => downloadData("mean_expression")} variant="outline" size="sm" className="h-6 px-2 text-xs">
                            <Download className="h-4 w-4 mr-2" /> Download CSV
                          </Button>
                        }
                      >
                        <div className="flex items-center space-x-2 mb-2 pl-6">
                          <Switch
                            id="mean-table-log2-switch"
                            checked={filterState.dataFormats.mean === "log2"}
                            onCheckedChange={(checked) => {
                              dispatch({
                                type: "SET_DATA_FORMAT",
                                payload: { metric: "mean", format: checked ? "log2" : "raw" },
                              });
                            }}
                            className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
                          />
                          <Label htmlFor="mean-table-log2-switch" className="text-sm text-blue-900">
                            Log<sub>2</sub>(X + 1)
                          </Label>
                        </div>
                        <DataTable
                          data={meanTableData}
                          columns={[
                            { key: "gene", header: "Gene", sortable: true },
                            ...filterState.selectedSites.flatMap(site => [
                              { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true },
                              { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true },
                            ]),
                          ]}
                          containerWidth="850px"
                          scrollHeight="450px"
                        />
                      </CollapsibleCard>

                      <CollapsibleCard
                        title="Gene Noise (CV)"
                        className="mb-4"
                        downloadButton={
                          <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
                            <Download className="h-4 w-4 mr-2" /> Download CSV
                          </Button>
                        }
                      >
                        <div className="flex items-center space-x-2 mb-2 pl-6">
                          <Switch
                            id="cv-table-log2-switch"
                            checked={filterState.dataFormats.cv === "log2"}
                            onCheckedChange={(checked) => {
                              dispatch({
                                type: "SET_DATA_FORMAT",
                                payload: { metric: "cv", format: checked ? "log2" : "raw" },
                              });
                            }}
                            className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
                          />
                          <Label htmlFor="cv-table-log2-switch" className="text-sm text-blue-900">
                            Log<sub>2</sub>(X + 1)
                          </Label>
                        </div>
                        <DataTable
                          data={cvTableData}
                          columns={[
                            { key: "gene", header: "Gene", sortable: true },
                            ...filterState.selectedSites.flatMap(site => [
                              { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true },
                              { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true },
                            ]),
                          ]}
                          containerWidth="850px"
                          scrollHeight="450px"
                        />
                      </CollapsibleCard>

                      <CollapsibleCard
                        title="Differential Noise (LogFC)"
                        className="mb-4"
                        downloadButton={
                          <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
                            <Download className="h-4 w-4 mr-2" /> Download CSV
                          </Button>
                        }
                      >
                        <DataTable
                          data={logfcTableData}
                          columns={[
                            { key: "gene", header: "Gene", sortable: true },
                            ...filterState.selectedSites.map(site => ({
                              key: site,
                              header: site,
                              render: (_, r) => renderValue(r[site]),
                              sortable: true,
                            })),
                          ]}
                          containerWidth="850px"
                          scrollHeight="450px"
                        />
                      </CollapsibleCard>
                      {filterState.selectedSites.length > 0 && logfcTableData.length > 0 && (
                        <CollapsibleCard
                          title={`LogFC ≥ ${filterState.logFCThreshold}`}
                          defaultOpen={false}
                          className="mb-6"
                        >
                          <div className="space-y-6">
                            {filterState.selectedSites.map((site) => {
                              const highLogFCGenes = logfcTableData
                                .map((row) => ({
                                  gene: row.gene,
                                  logfc: row[site],
                                }))
                                .filter((g) => g.logfc != null && Math.abs(g.logfc) >= filterState.logFCThreshold)
                                .map((g) => g.gene);

                              if (highLogFCGenes.length < 3) return null;

                              const matchingPathways = enrichedPathways.filter((p) =>
                                highLogFCGenes.every((g) => p.MatchingGenes.includes(g))
                              );

                              if (matchingPathways.length === 0) return null;

                              return (
                                <div key={site} className="border-l-4 border-blue-600 pl-4">
                                  <h4 className="font-semibold text-blue-900 mb-2">
                                    {site} ({highLogFCGenes.length} genes)
                                  </h4>
                                  <div className="mb-3">
                                    <p className="text-sm text-gray-700 mb-1">
                                      <strong>All high-LogFC genes:</strong>{" "}
                                      <span className="font-mono text-xs">{highLogFCGenes.join(", ")}</span>
                                    </p>
                                  </div>
                                  <div className="bg-blue-50 rounded p-3">
                                    <p className="text-sm font-medium text-blue-900 mb-2">
                                      Enriched Pathways:
                                    </p>

                                    {matchingPathways.length > 0 ? (
                                      <div className="space-y-1">
                                        {matchingPathways
                                          .sort((a, b) => a.FDR - b.FDR)
                                          .slice(0, 15)
                                          .map((pathway) => (
                                            <div
                                              key={pathway.Description}
                                              className="text-xs bg-white rounded px-2 py-1 flex justify-between items-center"
                                            >
                                              <span>
                                                <strong>{pathway.Description}</strong> ({pathway.Database})
                                              </span>
                                              <span className="text-gray-600">
                                                FDR: {formatFDR(pathway.FDR)}
                                              </span>
                                            </div>
                                          ))}
                                      </div>
                                    ) : (
                                      <p className="text-xs text-gray-600 italic">
                                        No enriched pathways found for these genes.
                                      </p>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                            {filterState.selectedSites.every((site) => {
                              const count = logfcTableData
                                .map((r) => r[site])
                                .filter((v) => v != null && Math.abs(v) >= filterState.logFCThreshold).length;
                              return count < 3;
                            }) && (
                              <div className="text-sm text-gray-600 italic text-center py-4">
                                No cancer site has 3 or more genes with LogFC ≥ {filterState.logFCThreshold}.
                              </div>
                            )}
                          </div>
                        </CollapsibleCard>
                      )}
                    </>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  );
};

export default React.memo(PathwayResults);
// import React, { useMemo, useCallback, useReducer, useEffect, useRef, useState } from "react";
// import { useSearchParams, Link } from "react-router-dom";
// import { Card, CardContent } from "@/components/ui/card";
// import { Button } from "@/components/ui/button";
// import { ArrowLeft, Download, Activity, Users, Info } from "lucide-react";
// import { PlotlyHeatmap } from "@/components/charts/index";
// import Header from "@/components/header";
// import Footer from "@/components/footer";
// import { useCache } from "@/hooks/use-cache";
// import { LoadingSpinner } from "@/components/ui/loadingSpinner";
// import FilterPanel from "@/components/FilterPanel";
// import { DataTable } from "@/components/ui/data-table";
// import CollapsibleCard from "@/components/ui/collapsible-card";
// import { Switch } from "@/components/ui/switch";
// import { Label } from "@/components/ui/label";
// import { Input } from "@/components/ui/input";
// import { Alert, AlertDescription } from "@/components/ui/alert";
// import SampleCounts from "@/components/SampleCounts";

// // ====================== INTERFACES ======================
// export interface GeneNoise {
//   mean_tumor: number | null;
//   mean_normal: number | null;
//   cv_tumor: number | null;
//   cv_normal: number | null;
//   logfc: number | null;
// }

// export interface GeneNoiseData {
//   [norm: string]: {
//     [gene: string]: {
//       [site: string]: GeneNoise;
//     };
//   };
// }

// export interface Enrichment {
//   Term: string;
//   Database: string;
//   FDR: number;
//   MatchingGenes: string[];
//   Description?: string;
//   GeneCount?: number;
//   EnrichmentScore?: number;
// }

// export interface EnrichmentResults {
//   enrichment: Enrichment[];
//   warning?: string | null;
// }

// export interface ResultsData {
//   raw: EnrichmentResults;
//   log2: EnrichmentResults;
// }

// interface FilterOption {
//   id: string;
//   label: string;
//   tooltip?: string;
// }

// interface FilterSection {
//   title: string;
//   id: string;
//   options: FilterOption[];
//   isMasterCheckbox?: boolean;
//   type: "checkbox" | "radio";
//   defaultOpen?: boolean;
//   customRender?: () => React.ReactNode;
// }

// interface FilterState {
//   selectedGroups: string[];
//   selectedSites: string[];
//   selectedGenes: string[];
//   normalizationMethod: string;
//   dataFormats: { mean: "raw" | "log2"; cv: "raw" | "log2"; logfc: "log2" };
//   selectedPathway: Enrichment | null;
//   sortBy: "fdr";
//   sortOrder: "asc" | "desc";
//   logFCThreshold: number;
//   isSampleCountsOpen: boolean;
//   analysisMode: "enrichment" | "neighbors";
//   geneInput: string;
//   showNetwork: boolean;
  
// }

// const initialFilterState: FilterState = {
//   selectedGroups: ["normal", "tumor"],
//   selectedSites: [],
//   selectedGenes: [],
//   normalizationMethod: "tpm",
//   dataFormats: { mean: "raw", cv: "raw", logfc: "log2" },
//   selectedPathway: null,
//   sortBy: "fdr",
//   sortOrder: "asc",
//   logFCThreshold: 0.7,
//   isSampleCountsOpen: true,
//   analysisMode: "enrichment",
//   geneInput: "",
//   showNetwork: false,
// };

// // ====================== REDUCER ======================
// type FilterAction =
//   | { type: "SET_GROUPS"; payload: string[] }
//   | { type: "SET_SITES"; payload: string[] }
//   | { type: "SET_GENES"; payload: string[] }
//   | { type: "SET_NORMALIZATION"; payload: string }
//   | { type: "SET_DATA_FORMAT"; payload: { metric: keyof FilterState["dataFormats"]; format: "raw" | "log2" } }
//   | { type: "SET_SELECTED_PATHWAY"; payload: Enrichment | null }
//   | { type: "SET_SORT_BY"; payload: "fdr" }
//   | { type: "SET_SORT_ORDER"; payload: "asc" | "desc" }
//   | { type: "SET_LOGFC_THRESHOLD"; payload: number }
//   | { type: "TOGGLE_SAMPLE_COUNTS" }
//   | { type: "SET_ANALYSIS_MODE"; payload: "enrichment" | "neighbors" }
//   | { type: "SET_GENE_INPUT"; payload: string }
//   | { type: "TOGGLE_NETWORK" };

// const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
//   switch (action.type) {
//     case "SET_GROUPS": return { ...state, selectedGroups: action.payload };
//     case "SET_SITES": return { ...state, selectedSites: action.payload };
//     case "SET_GENES": return { ...state, selectedGenes: action.payload };
//     case "SET_NORMALIZATION": return { ...state, normalizationMethod: action.payload };
//     case "SET_DATA_FORMAT":
//       if (action.payload.metric === "logfc") return state;
//       return { ...state, dataFormats: { ...state.dataFormats, [action.payload.metric]: action.payload.format } };
//     case "SET_SELECTED_PATHWAY": return { ...state, selectedPathway: action.payload };
//     case "SET_SORT_BY": return { ...state, sortBy: action.payload };
//     case "SET_SORT_ORDER": return { ...state, sortOrder: action.payload };
//     case "SET_LOGFC_THRESHOLD": return { ...state, logFCThreshold: action.payload };
//     case "TOGGLE_SAMPLE_COUNTS": return { ...state, isSampleCountsOpen: !state.isSampleCountsOpen };
//     case "SET_ANALYSIS_MODE": return { ...state, analysisMode: action.payload };
//     case "SET_GENE_INPUT": return { ...state, geneInput: action.payload };
//     case "TOGGLE_NETWORK": return { ...state, showNetwork: !state.showNetwork };
//     default: return state;
//   }
// };

// // ====================== ENRICHMENT HOOK WITH SMART CACHING ======================
// const useEnrichmentData = (
//   params: any,
//   filterState: FilterState,
//   areGenesSubsetOfInitial: (genes: string[]) => boolean
// ) => {
//   const [state, setState] = useState<{
//     resultsData: ResultsData;
//     isLoading: boolean;
//     error: string | null;
//     totalTumorSamples: number;
//     totalNormalSamples: number;
//     siteSampleCounts: { site: string; tumor: number; normal: number }[];
//     availableSites: { id: number; name: string }[];
//     fetchedSites: string[];
//   }>({
//     resultsData: { raw: { enrichment: [] }, log2: { enrichment: [] } },
//     isLoading: false,
//     error: null,
//     totalTumorSamples: 0,
//     totalNormalSamples: 0,
//     siteSampleCounts: [],
//     availableSites: [],
//     fetchedSites: [],
//   });

//   const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
//   const timeoutRef = useRef<NodeJS.Timeout | null>(null);
//   const initialGenesRef = useRef<string[]>(params.genes);
//   // const fetchEnrichment = useCallback(async (sites: string[]) => {
//   //   if (!sites.length) {
//   //     setState(s => ({ ...s, error: "Select at least one site", isLoading: false }));
//   //     return;
//   //   }

//   //   // Cache key includes genes, sites, library, mode
//   //   const cacheKey = generateCacheKey({
//   //     endpoint: "enrichment",
//   //     sites,
//   //     genes: filterState.selectedGenes,
//   //     library: params.library,
//   //     mode: filterState.analysisMode,
//   //   });

//   //   // 1. Try exact cache first
//   //   const cached = getCachedData(cacheKey);
//   //   if (cached) {
//   //     setState(s => ({ ...s, ...cached, isLoading: false, error: null }));
//   //     return;
//   //   }

//   //   // 2. If genes are subset of initial → try to reuse initial full payload
//   //   if (areGenesSubsetOfInitial(filterState.selectedGenes)) {
//   //     const initKey = generateCacheKey({
//   //       endpoint: "enrichment",
//   //       sites,
//   //       genes: initialGenesRef.current,
//   //       library: params.library,
//   //       mode: filterState.analysisMode,
//   //     });
//   //     const initCached = getCachedData(initKey);
//   //     if (initCached) {
//   //       setState(s => ({ ...s, ...initCached, isLoading: false, error: null }));
//   //       setCachedData(cacheKey, initCached); // cache filtered version
//   //       return;
//   //     }
//   //   }

//   //   // 3. No cache → fetch from API
//   //   setState(s => ({ ...s, isLoading: true }));
//   //   try {
//   //     const qp = new URLSearchParams({
//   //       cancer: sites.join(","),
//   //       top_n: "1000",
//   //       library: params.library,
//   //       mode: filterState.analysisMode,
//   //       ...(filterState.selectedGenes.length && { genes: filterState.selectedGenes.join(",") }),
//   //     });
//   //     const res = await fetch(`/api/pathway-analysis?${qp}`);
//   //     if (!res.ok) throw new Error(await res.text());
//   //     const data = await res.json();

//   //     const payload = {
//   //       resultsData: { raw: data.raw, log2: data.log2 } as ResultsData,
//   //       totalTumorSamples: Object.values(data.sample_counts).reduce((sum, c: any) => sum + (c.tumor ?? 0), 0) as number,
//   //       totalNormalSamples: Object.values(data.sample_counts).reduce((sum, c: any) => sum + (c.normal ?? 0), 0) as number,
//   //       siteSampleCounts: Object.entries(data.sample_counts).map(([site, c]: [string, any]) => ({
//   //         site,
//   //         tumor: c.tumor ?? 0,
//   //         normal: c.normal ?? 0,
//   //       })) as { site: string; tumor: number; normal: number }[],
//   //       availableSites: data.available_sites as { id: number; name: string }[],
//   //       fetchedSites: sites,
//   //     };
//   //     setCachedData(cacheKey, payload);
//   //     setState(s => ({ ...s, ...payload, isLoading: false, error: null }));
//   //   } catch (e: any) {
//   //     setState(s => ({ ...s, error: e.message, isLoading: false }));
//   //   }
//   // }, [
//   //   filterState.selectedGenes,
//   //   filterState.selectedSites,        
//   //   filterState.analysisMode,
//   //   params.library,
//   //   areGenesSubsetOfInitial,
//   //   getCachedData,
//   //   setCachedData,
//   //   generateCacheKey,
//   // ]);
// const fetchEnrichment = useCallback(async (sites: string[]) => {
//   if (!sites.length) {
//     setState((s) => ({ ...s, error: "Select at least one site", isLoading: false }));
//     return;
//   }

//   //Cache key now includes pathway ID if used
//   const cacheKey = generateCacheKey({
//     endpoint: "enrichment",
//     sites,
//     genes: filterState.selectedGenes,
//     pathway: filterState.selectedPathway?.Term || params.pathway,  // <-- pathway ID
//     library: params.library,
//     mode: filterState.analysisMode,
//   });

//   // Try cache
//   const cached = getCachedData(cacheKey);
//   if (cached) {
//     setState((s) => ({ ...s, ...cached, isLoading: false, error: null }));
//     return;
//   }

//   setState((s) => ({ ...s, isLoading: true }));

//   try {
//     // ✅ Build API query params
//     const qp = new URLSearchParams({
//       cancer: sites.join(","),
//       top_n: "1000",
//       mode: filterState.analysisMode,
//       ...(filterState.selectedGenes.length && { genes: filterState.selectedGenes.join(",") }),
//       ...(filterState.selectedPathway?.Term && { pathway: filterState.selectedPathway.Term }), // <-- Pass pathway ID
//     });

//     const res = await fetch(`/api/pathway-analysis?${qp}`);
//     if (!res.ok) throw new Error(await res.text());
//     const data = await res.json();

//     const payload = {
//       resultsData: { raw: data.raw, log2: data.log2 } as ResultsData,
//       totalTumorSamples: Object.values(data.sample_counts).reduce(
//         (sum: any, c: any) => sum + (c.tumor ?? 0),
//         0
//       ) as number,
//       totalNormalSamples: Object.values(data.sample_counts).reduce(
//         (sum: any, c: any) => sum + (c.normal ?? 0),
//         0
//       ) as number,
//       siteSampleCounts: Object.entries(data.sample_counts).map(
//         ([site, c]: [string, any]) => ({
//           site,
//           tumor: c.tumor ?? 0,
//           normal: c.normal ?? 0,
//         })
//       ) as { site: string; tumor: number; normal: number }[],
//       availableSites: data.available_sites as { id: number; name: string }[],
//       fetchedSites: sites,
//     };

//     setCachedData(cacheKey, payload);
//     setState((s) => ({ ...s, ...payload, isLoading: false, error: null }));
//   } catch (e: any) {
//     setState((s) => ({ ...s, error: e.message, isLoading: false }));
//   }
// }, [
//   filterState.selectedGenes,
//   filterState.selectedSites,
//   filterState.analysisMode,
//   filterState.selectedPathway,   // <-- new dependency
//   params.library,
//   getCachedData,
//   setCachedData,
//   generateCacheKey,
// ]);

//   const debounced = useCallback((sites: string[]) => {
//     if (timeoutRef.current) clearTimeout(timeoutRef.current);
//     timeoutRef.current = setTimeout(() => fetchEnrichment(sites), 500);
//   }, [fetchEnrichment]);

//   useEffect(() => {
//     if (filterState.selectedSites.length) {
//       debounced(filterState.selectedSites);
//     }
//   }, [filterState.selectedSites, debounced]);

//   return { ...state, refetchEnrichment: debounced };
// };

// // ====================== GENE NOISE HOOK ======================
// const useGeneNoise = (
//   sites: string[],
//   genes: string[],
//   normalizationMethod: string,
//   cancerTypes?: string[]
// ) => {
//   const [state, setState] = useState<{
//     data: GeneNoiseData;
//     isLoading: boolean;
//     error: string | null;
//   }>({
//     data: {},
//     isLoading: false,
//     error: null,
//   });

//   const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
//   const abortRef = useRef<AbortController | null>(null);

//   const fetchNoise = useCallback(async () => {
//     if (!sites.length || !genes.length) return;

//     const cacheKey = generateCacheKey({ endpoint: "gene-noise", sites, genes });
//     const cached = getCachedData(cacheKey);
//     if (cached) {
//       setState({ data: cached, isLoading: false, error: null });
//       return;
//     }

//     if (abortRef.current) abortRef.current.abort();
//     const ctrl = new AbortController();
//     abortRef.current = ctrl;

//     setState(s => ({ ...s, isLoading: true }));
//     try {
//       const qp = new URLSearchParams();
//       qp.set("cancer", sites.join(","));
//       if (genes && genes.length) qp.set("genes", genes.join(","));
//       if (cancerTypes && cancerTypes.length) qp.set("cancer_types", cancerTypes.join(","));
//       const url = `/api/gene-noise-pathway?${qp.toString()}`;

//       const res = await fetch(url, { signal: ctrl.signal });
//       if (!res.ok) throw new Error(await res.text());
//       const json = await res.json();
//       const payload = json.gene_noise;
//       setCachedData(cacheKey, payload);
//       setState({ data: payload, isLoading: false, error: null });
//     } catch (e: any) {
//       if (e.name !== "AbortError") setState(s => ({ ...s, isLoading: false, error: e.message }));
//     }
//   }, [sites, genes, getCachedData, setCachedData, generateCacheKey]);

//   const debounced = useMemo(() => {
//     let t: NodeJS.Timeout;
//     return () => { clearTimeout(t); t = setTimeout(fetchNoise, 300); };
//   }, [fetchNoise]);

//   return { ...state, refetchNoise: debounced };
// };

// // ====================== MAIN COMPONENT ======================
// const PathwayResults: React.FC = () => {
//   const [searchParams, setSearchParams] = useSearchParams();
//   const { getCachedData, generateCacheKey } = useCache<any>();

//   const params = useMemo(() => ({
//     sites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
//     cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
//     library: searchParams.get("library") || "KEGG_2021_Human",
//     genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
//     topN: parseInt(searchParams.get("top_n") || "1000", 10),
//     mode: searchParams.get("mode") || "enrichment",
//     pathway: searchParams.get("pathway"),
//   }), [searchParams]);

//   const initialGenesRef = useRef<string[]>(params.genes);

//   useEffect(() => { window.scrollTo(0, 0); }, []);

//   const [filterState, dispatch] = useReducer(filterReducer, {
//     ...initialFilterState,
//     selectedSites: params.sites,
//     selectedGenes: params.genes,
//     analysisMode: params.mode as "enrichment" | "neighbors",
//     geneInput: params.genes.join(", "),
//   });

//   // Helper: are current genes a subset of the initial URL genes?
//   const areGenesSubsetOfInitial = useCallback(
//     (genes: string[]) => {
//       if (!initialGenesRef.current.length) return false;
//       return genes.every(g => initialGenesRef.current.includes(g));
//     },
//     []
//   );

//   // === ENRICHMENT DATA ===
//   const {
//     resultsData,
//     isLoading: enrichLoading,
//     error: enrichError,
//     totalTumorSamples,
//     totalNormalSamples,
//     siteSampleCounts,
//     availableSites,
//     refetchEnrichment,
//   } = useEnrichmentData(params, filterState, areGenesSubsetOfInitial);

//   // === GENE NOISE DATA ===
//   const selectedGenesForNoise = filterState.selectedPathway
//     ? filterState.selectedPathway.MatchingGenes
//     : filterState.selectedGenes;

//   const { data: noiseData, isLoading: noiseLoading, refetchNoise } = useGeneNoise(
//     filterState.selectedSites,
//     selectedGenesForNoise,
//     filterState.normalizationMethod,
//     params.cancerTypes
//   );

//   // === SMART REFETCH LOGIC ===
//   const prevGenes = useRef<string[]>();
//   const prevSites = useRef<string[]>();

//   useEffect(() => {
//     const sitesChanged =
//       !prevSites.current ||
//       JSON.stringify(prevSites.current.sort()) !==
//         JSON.stringify(filterState.selectedSites.sort());

//     const genesChanged =
//       !prevGenes.current ||
//       JSON.stringify(prevGenes.current.sort()) !==
//         JSON.stringify(selectedGenesForNoise.sort());

//     const needFetch =
//       sitesChanged ||
//       (genesChanged && !areGenesSubsetOfInitial(selectedGenesForNoise));

//     if (needFetch) {
//       prevSites.current = filterState.selectedSites;
//       prevGenes.current = selectedGenesForNoise;
//       refetchNoise();
//     }
//   }, [
//     filterState.selectedSites,
//     selectedGenesForNoise,
//     areGenesSubsetOfInitial,
//     refetchNoise,
//   ]);

//   // === UPDATE FILTERS ===
//   const updateFilters = useCallback((updates: Partial<FilterState>) => {
//     if (updates.selectedSites !== undefined) dispatch({ type: "SET_SITES", payload: updates.selectedSites });
//     if (updates.selectedGenes !== undefined) dispatch({ type: "SET_GENES", payload: updates.selectedGenes });
//     if (updates.selectedPathway !== undefined) dispatch({ type: "SET_SELECTED_PATHWAY", payload: updates.selectedPathway });
//     if (updates.normalizationMethod !== undefined) dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
//     if (updates.dataFormats) {
//       Object.entries(updates.dataFormats).forEach(([metric, format]) => {
//         if (metric !== "logfc") {
//           dispatch({ type: "SET_DATA_FORMAT", payload: { metric: metric as keyof FilterState["dataFormats"], format: format as "raw" | "log2" } });
//         }
//       });
//     }

//     const newParams: any = { ...Object.fromEntries(searchParams) };
//     if (updates.selectedSites !== undefined) {
//       if (updates.selectedSites && updates.selectedSites.length) newParams.sites = updates.selectedSites.join(",");
//       else delete newParams.sites;
//     }
//     if (updates.selectedGenes !== undefined) {
//       if (updates.selectedGenes && updates.selectedGenes.length) newParams.genes = updates.selectedGenes.join(",");
//       else delete newParams.genes;
//     }
//     if (updates.selectedPathway !== undefined) {
//       if (updates.selectedPathway) newParams.pathway = updates.selectedPathway.Term;
//       else delete newParams.pathway;
//     }
//     setSearchParams(newParams);
//   }, [searchParams, setSearchParams]);

//   const handleGeneInputSubmit = useCallback(() => {
//     const genes = filterState.geneInput
//       .split(",")
//       .map((g) => g.trim().toUpperCase())
//       .filter((g) => g.length > 0);

//     updateFilters({ selectedGenes: genes, geneInput: genes.join(", "), selectedPathway: null });
//   }, [filterState.geneInput, updateFilters]);

//   const customFilters: FilterSection[] = useMemo(() => {
//     const availableGenes = filterState.selectedPathway
//       ? filterState.selectedPathway.MatchingGenes.map((gene) => ({ id: gene, label: gene }))
//       : [
//           ...new Set(
//             ["raw", "log2"].flatMap((scale) =>
//               ["tpm", "fpkm", "fpkm_uq"].flatMap((method) => Object.keys(resultsData[scale]?.[method]?.gene_stats || {}))
//             )
//           ),
//         ].map((gene) => ({ id: gene, label: gene }));

//     return [
//       {
//         title: "Sites",
//         id: "sites",
//         type: "checkbox",
//         options: availableSites.map((site) => ({
//           id: site.name,
//           label: site.name,
//           tooltip: `Cancer site: ${site.name}`,
//         })),
//         isMasterCheckbox: true,
//         defaultOpen: false,
//       },
//       {
//         title: "Genes",
//         id: "genes",
//         type: "checkbox",
//         options: availableGenes.length > 0 ? availableGenes : params.genes.map((gene) => ({ id: gene, label: gene })),
//         isMasterCheckbox: true,
//         defaultOpen: false,
//       },
//      {
//       title: "LogFC Threshold",
//       id: "logfc-threshold",
//       type: "checkbox", // irrelevant for custom
//       options: [],
//       defaultOpen: true,
//       customRender: () => (
//         <div className="flex items-center space-x-3 p-3 bg-gray-50 rounded-md">
//           <Label htmlFor="logfc-input" className="text-sm font-medium text-blue-900 whitespace-nowrap">
//             |Log₂FC| ≥
//           </Label>
//           <Input
//             id="logfc-input"
//             type="number"
//             min="0"
//             step="0.1"
//             value={filterState.logFCThreshold}
//             onChange={(e) => {
//               const raw = e.target.value;
//               const val = parseFloat(e.target.value);
//               if (raw === "") return;
//               if (!isNaN(val) && val >= 0) {
//                 dispatch({ type: "SET_LOGFC_THRESHOLD", payload: val });
//               }
//             }}
//             className="w-20 text-sm"
//           />
//           <span className="text-xs text-gray-600">(≥ 3 genes per cancer)</span>
//         </div>
//       ),
//     },
//     ];
//   }, [availableSites, resultsData, params.genes, filterState.selectedPathway, filterState.logFCThreshold]);



//   // === PATHWAY TABLE ===
//   const currentResults = resultsData[filterState.dataFormats.mean]?.[filterState.normalizationMethod] || { enrichment: [], gene_stats: {} } as any;
//   const currentEnrichment = currentResults.enrichment || [];
//   const enrichedPathways = useMemo(() => {
//     return (currentEnrichment as any[]).map((p: any, i: number) => {
//       const originalMatchingGenes: string[] = p.MatchingGenes ?? p.matchingGenes ?? p.inputGenes ?? p.inputGenesList ?? p.genes ?? [];
//       const Term: string = p.Term ?? p.term ?? p.value ?? "";
//       const FDR: number = p.FDR ?? p.fdr ?? p.fdr_value ?? 1;
//       const Database: string = p.Database ?? p.database ?? p.source ?? "";
//       const MatchingGenes: string[] = originalMatchingGenes;
//       const Description: string = p.Description ?? p.description ?? "";
//       const GeneCount: number = p.GeneCount ?? p.gene_count ?? (Array.isArray(MatchingGenes) ? MatchingGenes.length : 0);
//       const EnrichmentScore: number = p.EnrichmentScore ?? p.score ?? 0;

//       return {
//         Term,
//         Database,
//         FDR,
//         MatchingGenes,
//         Description,
//         GeneCount,
//         EnrichmentScore,
//         Rank: i + 1,
//       } as Enrichment & { Rank: number };
//     }).sort((a, b) => a.FDR - b.FDR);
//   }, [currentEnrichment]);

//   const formatFDR = (fdr: number) => fdr.toExponential(2);

//   // === RENDER HELPERS ===
//   const renderValue = (val: number | null | undefined) =>
//     val == null || Number.isNaN(Number(val)) ? "—" : Number(val).toFixed(3);

//   const norm = filterState.normalizationMethod;
//   const meanSource = resultsData[filterState.dataFormats.mean]?.[norm]?.gene_stats || {};
//   const cvSource = resultsData[filterState.dataFormats.cv]?.[norm]?.gene_stats || {};
//   const logfcSource = resultsData.log2?.[norm]?.gene_stats || {};

//   const meanTableData = useMemo(() => {
//     return Object.entries(meanSource).map(([gene, sites]) => ({
//       gene,
//       ...Object.fromEntries(
//         filterState.selectedSites.flatMap(site => {
//           const s = (sites as any)[site.toLowerCase()] || {};
//           return [
//             [`${site}_normal`, s.mean_normal ?? null],
//             [`${site}_tumor`, s.mean_tumor ?? null],
//           ];
//         })
//       ),
//     }));
//   }, [meanSource, filterState.selectedSites, filterState.dataFormats.mean, norm, resultsData]);

//   const logfcTableData = useMemo(() => {
//     return Object.entries(logfcSource).map(([gene, sites]) => ({
//       gene,
//       ...Object.fromEntries(
//         filterState.selectedSites.map(site => {
//           const siteKey = site.toLowerCase();
//           const siteStats = (sites as any)[siteKey] || {};
//           return [site, siteStats.logfc ?? null];
//         })
//       ),
//     }));
//   }, [logfcSource, filterState.selectedSites, norm]);

//   const isNoiseFullyLoaded = useMemo(() => {
//     if (!logfcSource || !filterState.selectedSites.length) return false;
//     return filterState.selectedSites.every(site => {
//       const lowerSite = site.toLowerCase();
//       return Object.values(logfcSource).some((stats: any) => {
//         const siteData = stats[lowerSite];
//         return siteData && siteData.logfc != null;
//       });
//     });
//   }, [logfcSource, filterState.selectedSites]);


//   const cvTableData = useMemo(() => {
//     return Object.entries(cvSource).map(([gene, sites]) => ({
//       gene,
//       ...Object.fromEntries(
//         filterState.selectedSites.flatMap(site => {
//           const s = (sites as any)[site.toLowerCase()] || {};
//           return [
//             [`${site}_normal`, s.cv_normal ?? null],
//             [`${site}_tumor`, s.cv_tumor ?? null],
//           ];
//         })
//       ),
//     }));
//   }, [cvSource, filterState.selectedSites, filterState.dataFormats.cv, norm, resultsData]);

//   // === DOWNLOAD ===
//   const downloadData = useCallback(
//     (type: "enrichment" | "mean_expression" | "noise_metrics") => {
//       const selectedSites = filterState.selectedSites as string[];
//       let data: any[] = [];
//       let headers: string[] = [];
//       let filename = `pathway_analysis_${selectedSites.join("_")}_${filterState.dataFormats.mean}_${filterState.normalizationMethod}`;
//       if (type === "enrichment") {
//         filename = `enrichment_results_${filterState.normalizationMethod}.csv`;
//         headers = ["Rank", "Pathway/GO Term", "Database", "FDR", "Matching Genes", "Description", "Gene Count"];
//         const rows = enrichedPathways.map((row) =>
//           [
//             row.Rank,
//             row.Term,
//             row.Database,
//             formatFDR(row.FDR),
//             row.MatchingGenes.join(" || "),
//             row.Description || "N/A",
//             row.GeneCount || 0,
//           ].join(",")
//         );
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       } else if (type === "mean_expression") {
//         data = Object.entries(currentResults.gene_stats);
//         headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])];
//         filename = `mean_expression_${filename}.csv`;
//         const rows = data.map(([gene, stats]) => {
//           const metrics = selectedSites
//             .map((site) => {
//               const lowerSite = site.toLowerCase();
//               const metric = stats[lowerSite] || {};
//               return [metric.mean_normal?.toFixed(2) || "0.00", metric.mean_tumor?.toFixed(2) || "0.00"];
//             })
//             .flat();
//           return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
//         });
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       } else if (type === "noise_metrics") {
//         data = Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {});
//         headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal CV`, `${site} Tumor CV`, `${site} Log2FC`])];
//         filename = `noise_metrics_${filename}.csv`;
//         const rows = data.map(([gene, stats]) => {
//           const metrics = selectedSites
//             .map((site) => {
//               const lowerSite = site.toLowerCase();
//               const metric = stats[lowerSite] || {};
//               return [
//                 metric.cv_normal?.toFixed(2) || "0.00",
//                 metric.cv_tumor?.toFixed(2) || "0.00",
//                 metric.logfc?.toFixed(2) || "0.00",
//               ];
//             })
//             .flat();
//           return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
//         });
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       }
//     },
//     [enrichedPathways, filterState.normalizationMethod, resultsData, filterState.dataFormats, filterState.selectedSites]
//   );

//   // === Z-SCORES ===
//   const getZValues = useCallback((key: "mean" | "cv" | "logfc") => {
//     const source =
//       key === "mean"
//         ? resultsData[filterState.dataFormats.mean][norm]?.gene_stats
//         : key === "cv"
//         ? resultsData[filterState.dataFormats.cv][norm]?.gene_stats
//         : resultsData.log2[filterState.normalizationMethod]?.gene_stats;

//     if (!source) return [];

//     const values: number[] = [];
//     Object.entries(source).forEach(([_, sites]) => {
//       filterState.selectedSites.forEach(site => {
//         const s = (sites as any)[site.toLowerCase()] || {};
//         if (key === "mean") {
//           if (s.mean_normal != null) values.push(s.mean_normal);
//           if (s.mean_tumor != null) values.push(s.mean_tumor);
//         } else if (key === "cv") {
//           if (s.cv_normal != null) values.push(s.cv_normal);
//           if (s.cv_tumor != null) values.push(s.cv_tumor);
//         } else if (key === "logfc" && s.logfc != null) {
//           values.push(s.logfc);
//         }
//       });
//     });

//     const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
//     const std = values.length > 1
//       ? Math.sqrt(values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (values.length - 1))
//       : 0;

//     return Object.keys(source).map(gene => {
//       const sites = source[gene] || {};
//       return filterState.selectedSites.flatMap(site => {
//         const s = sites[site.toLowerCase()] || {};
//         const z = (v: number | null) => v == null ? null : std > 0 ? (v - mean) / std : 0;
//         if (key === "logfc") return [s.logfc != null ? z(s.logfc) : null];
//         return [z(s.mean_normal), z(s.mean_tumor)];
//       });
//     });
//   }, [
//     resultsData,
//     filterState.dataFormats,
//     filterState.selectedSites,
//     filterState.normalizationMethod,
//     norm,
//   ]);

//   // === WARNING: NO NORMAL SAMPLES ===
//   const hasNoNormal = filterState.selectedSites.some(site => {
//     const c = siteSampleCounts.find(s => s.site === site);
//     return c && c.normal === 0;
//   });

//   return (
//     <div className="min-h-screen bg-white flex flex-col">
//       <Header />
//       <main className="flex-grow">
//         <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
//           <div className="grid grid-cols-[320px_1fr] gap-6">
//             <FilterPanel
//               normalizationMethod={filterState.normalizationMethod}
//               setNormalizationMethod={(value) => updateFilters({ normalizationMethod: value })}
//               customFilters={customFilters}
//               onFilterChange={(filterId, value) => {
//                 if (filterId === "sites") {
//                   updateFilters({ selectedSites: value });
//                 } else if (filterId === "genes") {
//                   updateFilters({ selectedGenes: value });
//                 }
//               }}
//               selectedValues={{
//                 sites: filterState.selectedSites,
//                 genes: filterState.selectedGenes,
//               }}
//             />

//             <div className="flex-1">
//               {enrichLoading || noiseLoading ? (
//                 <LoadingSpinner message="Loading..." />
//               ) : enrichError ? (
//                 <div className="text-red-600">{enrichError}</div>
//               ) : filterState.selectedSites.length === 0 ? (
//                 <div className="text-red-600">Please select at least one site.</div>
//               ) : (
//                 <>
//                   <Link
//                     to="/pathway-analysis"
//                     className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
//                   >
//                     <ArrowLeft className="h-4 w-4 mr-2" />
//                     Back to Pathway Analysis
//                   </Link>
//                   <div className="mb-8">
//                     <div className="flex items-center justify-between mb-6">
//                       <h2 className="text-4xl font-bold text-blue-900">Results For Pathway Analysis</h2>
//                       {/* <Button onClick={() => downloadData("enrichment")} variant="outline" size="sm">
//                         <Download className="h-4 w-4 mr-2" /> Download Enrichment CSV
//                       </Button> */}
//                     </div>
//                     <div className="overflow-x-auto mb-6">
//                       <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
//                         <tbody>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Normalization</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.normalizationMethod.toUpperCase()}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Genes</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedGenes.join(", ") || "None"}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Selected Pathway</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedPathway?.Description || "None"}</td>
//                           </tr>
//                           <tr>
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Cancer Site(s)</th>
//                             <td className="py-3 px-4 text-blue-700">
//                               {filterState.selectedSites.join(", ")}
//                               {params.cancerTypes.length > 0 && ` (${params.cancerTypes.join(", ")})`}
//                             </td>
//                           </tr>
//                         </tbody>
//                       </table>
//                       {hasNoNormal && (
//                         <Alert className="mb-4">
//                           <AlertDescription>Warning: Some sites have no normal samples. CV-normal and logFC will show "—".</AlertDescription>
//                         </Alert>
//                       )}
//                     </div>
//                   </div>

//                   <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-green-600 mb-2" />
//                         <div className="text-2xl font-bold text-green-600">{totalNormalSamples}</div>
//                         <div className="text-xs text-gray-600">Total Normal Samples</div>
//                       </CardContent>
//                     </Card>
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-red-600 mb-2" />
//                         <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
//                         <div className="text-xs text-gray-600">Total Tumor Samples</div>
//                       </CardContent>
//                     </Card>
//                   </div>

//                   <SampleCounts
//                     isOpen={filterState.isSampleCountsOpen}
//                     toggleOpen={() => dispatch({ type: "TOGGLE_SAMPLE_COUNTS" })}
//                     siteSampleCounts={siteSampleCounts}
//                     selectedSites={filterState.selectedSites}
//                     selectedGroups={filterState.selectedGroups}
//                   />

//                   {currentResults.enrichment.length === 0 ? (
//                     <Card className="shadow-lg p-6 text-center text-blue-700">
//                       <Activity className="w-10 h-10 mx-auto mb-3" />
//                       <p className="text-lg">No pathway enrichment results found for the selected parameters.</p>
//                     </Card>
//                   ) : (
//                     <>
//                       <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
//                         {/* <div className="lg:col-span-2">
//                           <CollapsibleCard title="Enriched Pathways" className="h-full">
//                             <DataTable
//                               data={enrichedPathways}
//                               columns={[
//                                 { key: "Rank", header: "Rank", sortable: true },
//                                 { key: "Term", header: "Pathway", sortable: true },
//                                 {
//                                   key: "FDR",
//                                   header: "FDR",
//                                   sortable: true,
//                                   render: (value: number) => formatFDR(value),
//                                 },
//                               ]}
//                               scrollHeight="450px"
//                               stickyHeader
//                               defaultSortKey="FDR"
//                               defaultSortOrder="asc"
//                               onRowClick={(row: Enrichment) => {
//                                 const currentlySelected = filterState.selectedPathway?.Term === row.Term;
//                                 const newPathway = currentlySelected ? null : row;

//                                 dispatch({ type: "SET_SELECTED_PATHWAY", payload: newPathway });

//                                 const newParams: any = { ...Object.fromEntries(searchParams) };
//                                 if (newPathway) {
//                                   newParams.pathway = newPathway.Term;
//                                 } else {
//                                   delete newParams.pathway;
//                                 }
//                                 setSearchParams(newParams);
//                               }}
//                               containerWidth="565px"
//                               rowClassName={(row) => {
//                                 const isSelected = filterState.selectedPathway?.Term === row.Term;
//                                 return isSelected ? "bg-blue-100 font-semibold" : "";
//                               }}
//                             />
//                           </CollapsibleCard>
//                         </div> */}
//                         {/* <div>
//                           <CollapsibleCard title="Pathway Details" className="h-full">
//                             {filterState.selectedPathway ? (
//                               <div className="space-y-2 p-2">
//                                 <div>
//                                   <h3 className="font-semibold text-blue-700 text-sm">{filterState.selectedPathway.Term}</h3>
//                                   <p className="text-xs text-gray-600">Database: {filterState.selectedPathway.Database}</p>
//                                   <p className="text-xs text-gray-600">Description: {filterState.selectedPathway.Description || "N/A"}</p>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Statistics</h4>
//                                   <div className="grid grid-cols-2 gap-1 mt-1 text-xs">
//                                     <div>FDR:</div>
//                                     <div>{formatFDR(filterState.selectedPathway.FDR)}</div>
//                                     <div>Gene Count:</div>
//                                     <div>{filterState.selectedPathway.GeneCount}</div>
//                                   </div>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Matching Genes</h4>
//                                   <div className="flex flex-wrap gap-1 mt-1">
//                                     {filterState.selectedPathway.MatchingGenes.map((gene, idx) => (
//                                       <span key={idx} className="bg-blue-100 text-blue-800 px-1 py-0.5 rounded text-xs">
//                                         {gene}
//                                       </span>
//                                     ))}
//                                   </div>
//                                 </div>
//                               </div>
//                             ) : (
//                               <div className="text-center text-gray-500 h-full flex items-center justify-center">
//                                 <div>
//                                   <Info className="h-6 w-6 mx-auto mb-1" />
//                                   <p className="text-xs">Select a pathway to view details</p>
//                                 </div>
//                               </div>
//                             )}
//                           </CollapsibleCard>
//                         </div> */}
//                       </div>

//                       { (
//                         <>
//                           <CollapsibleCard title="Mean Expression" className="mb-4">
//                             <PlotlyHeatmap
//                               title="Mean Expression (Z-scores)"
//                               data={Object.entries(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("mean")}
//                               zLabel={`Mean Expression (${filterState.dataFormats.mean.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="expression-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                             />
//                           </CollapsibleCard>

//                           <CollapsibleCard title="Coefficient of Variation (CV)" className="mb-4">
//                             <PlotlyHeatmap
//                               title="Coefficient of Variation (Z-scores)"
//                               data={Object.entries(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("cv")}
//                               zLabel={`Noise (${filterState.dataFormats.cv.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="cv-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                             />
//                           </CollapsibleCard>

//                           <CollapsibleCard title="Differential Noise" className="mb-4">
//                             <PlotlyHeatmap
//                               title={`Log₂(CV<sub>tumor</sub> / CV<sub>normal</sub>)`}
//                               data={Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites}
//                               yValues={Object.keys(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("logfc")}
//                               zLabel="Log₂FC"
//                               chartKey="logfc-heatmap"
//                               xLabel="Cancer Sites"
//                               yLabel="Genes"
//                             />
//                           </CollapsibleCard>

//                           <CollapsibleCard
//                             title="Mean Expression"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("mean_expression")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="mean-table-log2-switch"
//                                 checked={filterState.dataFormats.mean === "log2"}
//                                 onCheckedChange={(checked) => {
//                                   dispatch({
//                                     type: "SET_DATA_FORMAT",
//                                     payload: { metric: "mean", format: checked ? "log2" : "raw" },
//                                   });
//                                 }}
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="mean-table-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                             <DataTable
//                               data={meanTableData}
//                               columns={[
//                                 { key: "gene", header: "Gene", sortable: true },
//                                 ...filterState.selectedSites.flatMap(site => [
//                                   { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true },
//                                   { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true },
//                                 ]),
//                               ]}
//                               containerWidth="850px"
//                               scrollHeight="450px"
//                             />
//                           </CollapsibleCard>

//                           <CollapsibleCard
//                             title="Gene Noise (CV)"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="cv-table-log2-switch"
//                                 checked={filterState.dataFormats.cv === "log2"}
//                                 onCheckedChange={(checked) => {
//                                   dispatch({
//                                     type: "SET_DATA_FORMAT",
//                                     payload: { metric: "cv", format: checked ? "log2" : "raw" },
//                                   });
//                                 }}
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="cv-table-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                             <DataTable
//                               data={cvTableData}
//                               columns={[
//                                 { key: "gene", header: "Gene", sortable: true },
//                                 ...filterState.selectedSites.flatMap(site => [
//                                   { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true },
//                                   { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true },
//                                 ]),
//                               ]}
//                               containerWidth="850px"
//                               scrollHeight="450px"
//                             />
//                           </CollapsibleCard>

//                           <CollapsibleCard
//                             title="Differential Noise (LogFC)"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <DataTable
//                               data={logfcTableData}
//                               columns={[
//                                 { key: "gene", header: "Gene", sortable: true },
//                                 ...filterState.selectedSites.map(site => ({
//                                   key: site,
//                                   header: site,
//                                   render: (_, r) => renderValue(r[site]),
//                                   sortable: true,
//                                 })),
//                               ]}
//                               containerWidth="850px"
//                               scrollHeight="450px"
//                             />
//                           </CollapsibleCard>
//                           {filterState.selectedSites.length > 0 && logfcTableData.length > 0 && isNoiseFullyLoaded && (
//                             <CollapsibleCard
//                               title={`LogFC ≥ ${filterState.logFCThreshold})`}
//                               defaultOpen={false}
//                               className="mb-6"
//                             >
//                               <div className="space-y-6">
//                                 {filterState.selectedSites.map((site) => {
//                                   const highLogFCGenes = logfcTableData
//                                     .map((row) => ({
//                                       gene: row.gene,
//                                       logfc: row[site],
//                                     }))
//                                     .filter((g) => g.logfc != null && Math.abs(g.logfc) >= filterState.logFCThreshold)
//                                     .map((g) => g.gene);

//                                   if (highLogFCGenes.length < 3) return null;

//                                   const matchingPathways = enrichedPathways.filter((p) =>
//                                     highLogFCGenes.every((g) => p.MatchingGenes.includes(g))
//                                   );

//                                   if (matchingPathways.length === 0) return null;

//                                   return (
//                                     <div key={site} className="border-l-4 border-blue-600 pl-4">
//                                       <h4 className="font-semibold text-blue-900 mb-2">
//                                         {site} ({highLogFCGenes.length} genes)
//                                       </h4>
//                                       <div className="mb-3">
//                                         <p className="text-sm text-gray-700 mb-1">
//                                           <strong>All high-LogFC genes:</strong>{" "}
//                                           <span className="font-mono text-xs">{highLogFCGenes.join(", ")}</span>
//                                         </p>
//                                       </div>
//                                       <div className="bg-blue-50 rounded p-3">
//                                         <p className="text-sm font-medium text-blue-900 mb-2">
//                                           Enriched Pathways (Top 5 by FDR):
//                                         </p>
//                                         <div className="space-y-1">
//                                          {matchingPathways
//                                           .sort((a, b) => a.FDR - b.FDR) // Sort by FDR ascending
//                                           .slice(0, 5) // Take only top 5
//                                           .map((pathway) => (
//                                             <div
//                                               key={pathway.Description}
//                                               className="text-xs bg-white rounded px-2 py-1 flex justify-between items-center"
//                                             >
//                                               <span>
//                                                 <strong>{pathway.Description}</strong> ({pathway.Database})
//                                               </span>
//                                               <span className="text-gray-600">
//                                                 FDR: {formatFDR(pathway.FDR)}
//                                               </span>
//                                             </div>
//                                           ))}
//                                         </div>
//                                       </div>
//                                     </div>
//                                   );
//                                 })}
//                                 {filterState.selectedSites.every((site) => {
//                                   const count = logfcTableData
//                                     .map((r) => r[site])
//                                     .filter((v) => v != null && Math.abs(v) >= filterState.logFCThreshold).length;
//                                   return count < 3;
//                                 }) && (
//                                   <div className="text-sm text-gray-600 italic text-center py-4">
//                                     No cancer site has 3 or more genes with LogFC ≥ {filterState.logFCThreshold}.
//                                   </div>
//                                 )}
//                               </div>
//                             </CollapsibleCard>
//                           )}
//                         </>
//                       )}
//                     </>
//                   )}
//                 </>
//               )}
//             </div>
//           </div>
//         </div>
//       </main>
//       <Footer />
//     </div>
//   );
// };

// export default React.memo(PathwayResults);
// import React, { useMemo, useCallback, useReducer, useEffect, useRef, useState } from "react";
// import { useSearchParams, Link } from "react-router-dom";
// import { Card, CardContent } from "@/components/ui/card";
// import { Button } from "@/components/ui/button";
// import { ArrowLeft, Download, Activity, Users, Info } from "lucide-react";
// import { PlotlyHeatmap } from "@/components/charts/index";
// import Header from "@/components/header";
// import Footer from "@/components/footer";
// import { useCache } from "@/hooks/use-cache";
// import { LoadingSpinner } from "@/components/ui/loadingSpinner";
// import FilterPanel from "@/components/FilterPanel";
// import { DataTable } from "@/components/ui/data-table";
// import CollapsibleCard from "@/components/ui/collapsible-card";
// import { Switch } from "@/components/ui/switch";
// import { Label } from "@/components/ui/label";
// import { Input } from "@/components/ui/input";
// import { Alert, AlertDescription } from "@/components/ui/alert";
// import SampleCounts from "@/components/SampleCounts";

// // ====================== INTERFACES ======================
// export interface GeneNoise {
//   mean_tumor: number | null;
//   mean_normal: number | null;
//   cv_tumor: number | null;
//   cv_normal: number | null;
//   logfc: number | null;
// }

// export interface GeneNoiseData {
//   [norm: string]: {
//     [gene: string]: {
//       [site: string]: GeneNoise;
//     };
//   };
// }

// export interface Enrichment {
//   Term: string;
//   Database: string;
//   FDR: number;
//   MatchingGenes: string[];
//   Description?: string;
//   GeneCount?: number;
//   EnrichmentScore?: number;
// }

// export interface EnrichmentResults {
//   enrichment: Enrichment[];
//   warning?: string | null;
// }

// export interface ResultsData {
//   raw: EnrichmentResults;
//   log2: EnrichmentResults;
// }


// interface FilterOption {
//   id: string;
//   label: string;
//   tooltip?: string;
// }

// interface FilterSection {
//   title: string;
//   id: string;
//   options: FilterOption[];
//   isMasterCheckbox?: boolean;
//   type: "checkbox" | "radio";
//   defaultOpen?: boolean;
// }

// interface FilterState {
//   selectedGroups: string[];
//   selectedSites: string[];
//   selectedGenes: string[];
//   normalizationMethod: string;
//   dataFormats: { mean: "raw" | "log2"; cv: "raw" | "log2"; logfc: "log2" };
//   selectedPathway: Enrichment | null;
//   sortBy: "fdr";
//   sortOrder: "asc" | "desc";
//   logFCThreshold: number;
//   isSampleCountsOpen: boolean;
//   analysisMode: "enrichment" | "neighbors";
//   geneInput: string;
//   showNetwork: boolean;
// }

// const initialFilterState: FilterState = {
//   selectedGroups: ["normal", "tumor"],
//   selectedSites: [],
//   selectedGenes: [],
//   normalizationMethod: "tpm",
//   dataFormats: { mean: "raw", cv: "raw", logfc: "log2" },
//   selectedPathway: null,
//   sortBy: "fdr",
//   sortOrder: "asc",
//   logFCThreshold: 0.7,
//   isSampleCountsOpen: true,
//   analysisMode: "enrichment",
//   geneInput: "",
//   showNetwork: false,
// };

// // ====================== REDUCER ======================
// type FilterAction =
//   | { type: "SET_GROUPS"; payload: string[] }
//   | { type: "SET_SITES"; payload: string[] }
//   | { type: "SET_GENES"; payload: string[] }
//   | { type: "SET_NORMALIZATION"; payload: string }
//   | { type: "SET_DATA_FORMAT"; payload: { metric: keyof FilterState["dataFormats"]; format: "raw" | "log2" } }
//   | { type: "SET_SELECTED_PATHWAY"; payload: Enrichment | null }
//   | { type: "SET_SORT_BY"; payload: "fdr" }
//   | { type: "SET_SORT_ORDER"; payload: "asc" | "desc" }
//   | { type: "SET_LOGFC_THRESHOLD"; payload: number }
//   | { type: "TOGGLE_SAMPLE_COUNTS" }
//   | { type: "SET_ANALYSIS_MODE"; payload: "enrichment" | "neighbors" }
//   | { type: "SET_GENE_INPUT"; payload: string }
//   | { type: "TOGGLE_NETWORK" };

// const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
//   switch (action.type) {
//     case "SET_GROUPS": return { ...state, selectedGroups: action.payload };
//     case "SET_SITES": return { ...state, selectedSites: action.payload };
//     case "SET_GENES": return { ...state, selectedGenes: action.payload };
//     case "SET_NORMALIZATION": return { ...state, normalizationMethod: action.payload };
//     case "SET_DATA_FORMAT":
//       if (action.payload.metric === "logfc") return state;
//       return { ...state, dataFormats: { ...state.dataFormats, [action.payload.metric]: action.payload.format } };
//     case "SET_SELECTED_PATHWAY": return { ...state, selectedPathway: action.payload };
//     case "SET_SORT_BY": return { ...state, sortBy: action.payload };
//     case "SET_SORT_ORDER": return { ...state, sortOrder: action.payload };
//     case "SET_LOGFC_THRESHOLD": return { ...state, logFCThreshold: action.payload };
//     case "TOGGLE_SAMPLE_COUNTS": return { ...state, isSampleCountsOpen: !state.isSampleCountsOpen };
//     case "SET_ANALYSIS_MODE": return { ...state, analysisMode: action.payload };
//     case "SET_GENE_INPUT": return { ...state, geneInput: action.payload };
//     case "TOGGLE_NETWORK": return { ...state, showNetwork: !state.showNetwork };
//     default: return state;
//   }
// };

// // ====================== ENRICHMENT HOOK ======================
// const useEnrichmentData = (
//   params: any,
//   filterState: FilterState
// ) => {
//   const [state, setState] = useState<{
//     resultsData: ResultsData;
//     isLoading: boolean;
//     error: string | null;
//     totalTumorSamples: number;
//     totalNormalSamples: number;
//     siteSampleCounts: { site: string; tumor: number; normal: number }[];
//     availableSites: { id: number; name: string }[];
//     fetchedSites: string[];
//   }>({
//     resultsData: { raw: { enrichment: [] }, log2: { enrichment: [] } },
//     isLoading: false,
//     error: null,
//     totalTumorSamples: 0,
//     totalNormalSamples: 0,
//     siteSampleCounts: [],
//     availableSites: [],
//     fetchedSites: [],
//   });

//   const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
//   const timeoutRef = useRef<NodeJS.Timeout | null>(null);

//   const fetchEnrichment = useCallback(async (sites: string[]) => {
//     if (!sites.length) {
//       setState(s => ({ ...s, error: "Select at least one site", isLoading: false }));
//       return;
//     }

//     const cacheKey = generateCacheKey({ endpoint: "enrichment", sites, genes: filterState.selectedGenes });
//     const cached = getCachedData(cacheKey);
//     if (cached) {
//       setState(s => ({ ...s, ...cached, isLoading: false, error: null }));
//       return;
//     }

//     setState(s => ({ ...s, isLoading: true }));
//     try {
//       const qp = new URLSearchParams({
//         cancer: sites.join(","),
//         top_n: "1000",
//         library: params.library,
//         mode: filterState.analysisMode,
//         ...(filterState.selectedGenes.length && { genes: filterState.selectedGenes.join(",") }),
//       });
//       const res = await fetch(`/api/pathway-analysis?${qp}`);
//       if (!res.ok) throw new Error(await res.text());
//       const data = await res.json();

//       // const payload = {
//       //   resultsData: { raw: data.raw, log2: data.log2 },
//       //   totalTumorSamples: Object.values(data.sample_counts).reduce((a: any, c: any) => a + c.tumor, 0),
//       //   totalNormalSamples: Object.values(data.sample_counts).reduce((a: any, c: any) => a + c.normal, 0),
//       //   siteSampleCounts: Object.entries(data.sample_counts).map(([site, c]: any) => ({ site, ...c })),
//       //   availableSites: data.available_sites,
//       //   fetchedSites: sites,
//       // };
//       const payload = {
//         resultsData: { raw: data.raw, log2: data.log2 } as ResultsData,
//         totalTumorSamples: Object.values(data.sample_counts).reduce((sum, c: any) => sum + (c.tumor ?? 0), 0) as number,
//         totalNormalSamples: Object.values(data.sample_counts).reduce((sum, c: any) => sum + (c.normal ?? 0), 0) as number,
//         siteSampleCounts: Object.entries(data.sample_counts).map(([site, c]: [string, any]) => ({
//           site,
//           tumor: c.tumor ?? 0,
//           normal: c.normal ?? 0,
//         })) as { site: string; tumor: number; normal: number }[],
//         availableSites: data.available_sites as { id: number; name: string }[],
//         fetchedSites: sites,
//       };
//       setCachedData(cacheKey, payload);
//       setState(s => ({ ...s, ...payload, isLoading: false, error: null }));
//     } catch (e: any) {
//       setState(s => ({ ...s, error: e.message, isLoading: false }));
//     }
//   }, [filterState.selectedGenes, filterState.analysisMode, params.library, getCachedData, setCachedData, generateCacheKey]);

//   const debounced = useCallback((sites: string[]) => {
//     if (timeoutRef.current) clearTimeout(timeoutRef.current);
//     timeoutRef.current = setTimeout(() => fetchEnrichment(sites), 500);
//   }, [fetchEnrichment]);

//   useEffect(() => {
//     // const newSites = filterState.selectedSites.filter(s => !state.fetchedSites.includes(s));
//     // if (newSites.length) debounced(newSites);
//     // Always fetch for the full set of selected sites so server returns
//     // a complete payload that the UI expects (not just the incremental subset).
//     if (filterState.selectedSites.length) {
//       debounced(filterState.selectedSites);
//     }
//   }, [filterState.selectedSites, debounced, state.fetchedSites]);

//   return { ...state, refetchEnrichment: debounced };
// };

// // ====================== GENE NOISE HOOK ======================
// const useGeneNoise = (
//   sites: string[],
//   genes: string[],
//   normalizationMethod: string,
//   cancerTypes?: string[]
// ) => {
//   const [state, setState] = useState<{
//     data: GeneNoiseData;
//     isLoading: boolean;
//     error: string | null;
//   }>({
//     data: {},
//     isLoading: false,
//     error: null,
//   });

//   const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();
//   const abortRef = useRef<AbortController | null>(null);

//   const fetchNoise = useCallback(async () => {
//     if (!sites.length || !genes.length) return;

//     const cacheKey = generateCacheKey({ endpoint: "gene-noise", sites, genes });
//     const cached = getCachedData(cacheKey);
//     if (cached) {
//       setState({ data: cached, isLoading: false, error: null });
//       return;
//     }

//     if (abortRef.current) abortRef.current.abort();
//     const ctrl = new AbortController();
//     abortRef.current = ctrl;

//     setState(s => ({ ...s, isLoading: true }));
//     try {
//       // Build repeated query params (server accepts repeated or comma-separated)
//       // const qp = new URLSearchParams();
//       // sites.forEach(s => qp.append("cancer", s));
//       // genes.forEach(g => qp.append("genes", g));
//       // if (cancerTypes && cancerTypes.length) cancerTypes.forEach(ct => qp.append("cancer_types", ct));
//       // const url = `/api/gene-noise-pathway?${qp.toString()}`;
//       // Build query params as comma-separated strings so backend parsing works reliably
//       const qp = new URLSearchParams();
//       qp.set("cancer", sites.join(","));
//       if (genes && genes.length) qp.set("genes", genes.join(","));
//       if (cancerTypes && cancerTypes.length) qp.set("cancer_types", cancerTypes.join(","));
//       const url = `/api/gene-noise-pathway?${qp.toString()}`;

//       console.debug("[useGeneNoise] fetching", url);
//       const res = await fetch(url, { signal: ctrl.signal });
//       if (!res.ok) throw new Error(await res.text());
//       const json = await res.json();
//       const payload = json.gene_noise;
//       setCachedData(cacheKey, payload);
//       setState({ data: payload, isLoading: false, error: null });
//     } catch (e: any) {
//       if (e.name !== "AbortError") setState(s => ({ ...s, isLoading: false, error: e.message }));
//     }
//   }, [sites, genes, getCachedData, setCachedData, generateCacheKey]);

//   const debounced = useMemo(() => {
//     let t: NodeJS.Timeout;
//     return () => { clearTimeout(t); t = setTimeout(fetchNoise, 300); };
//   }, [fetchNoise]);

//   return { ...state, refetchNoise: debounced };
// };


// // ====================== MAIN COMPONENT ======================
// const PathwayResults: React.FC = () => {
//   const [searchParams, setSearchParams] = useSearchParams();
//   const { getCachedData, generateCacheKey } = useCache<any>();

//   const params = useMemo(() => ({
//     sites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
//     cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
//     library: searchParams.get("library") || "KEGG_2021_Human",
//     genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
//     topN: parseInt(searchParams.get("top_n") || "1000", 10),
//     mode: searchParams.get("mode") || "enrichment",
//     pathway: searchParams.get("pathway"),
//   }), [searchParams]);
//   const initialGenesRef = useRef<string[]>(params.genes);

//   useEffect(() => { window.scrollTo(0, 0); }, []);

//   const [filterState, dispatch] = useReducer(filterReducer, {
//     ...initialFilterState,
//     selectedSites: params.sites,
//     selectedGenes: params.genes,
//     analysisMode: params.mode as "enrichment" | "neighbors",
//     geneInput: params.genes.join(", "),
//   });
//   const areGenesSubsetOfInitial = (genes: string[]) => {
//     if (!initialGenesRef.current.length) return false;               // no initial → always fetch
//     return genes.every(g => initialGenesRef.current.includes(g));
//   };

//   // === ENRICHMENT DATA ===
//   const {
//     resultsData,
//     isLoading: enrichLoading,
//     error: enrichError,
//     totalTumorSamples,
//     totalNormalSamples,
//     siteSampleCounts,
//     availableSites,
//     refetchEnrichment,
//   } = useEnrichmentData(params, filterState);

//   // === GENE NOISE DATA ===
//   const selectedGenesForNoise = filterState.selectedPathway
//     ? filterState.selectedPathway.MatchingGenes
//     : filterState.selectedGenes;

//   const { data: noiseData, isLoading: noiseLoading, refetchNoise } = useGeneNoise(
//     filterState.selectedSites,
//     selectedGenesForNoise,
//     filterState.normalizationMethod,
//     params.cancerTypes
//   );

//   // === AUTO-TRIGGER NOISE FETCH ON GENE/SITE CHANGE ===
//   const prevGenes = useRef<string[]>();
//   const prevSites = useRef<string[]>();

//   useEffect(() => {
//     if (!filterState.selectedPathway) return;
//     const sites = filterState.selectedSites.length ? filterState.selectedSites : params.sites;
//     // call debounced enrichment fetch (expects sites array)
//     try { refetchEnrichment?.(sites); } catch { /* ignore */ }
//     // refetch gene-noise (debounced function returned from useGeneNoise)
//     try { refetchNoise(); } catch { /* ignore */ }
//   }, [filterState.selectedPathway, filterState.selectedSites, params.sites, refetchEnrichment, refetchNoise]);
//   useEffect(() => {
//     const genesChanged = !prevGenes.current ||
//       JSON.stringify(prevGenes.current.sort()) !== JSON.stringify(selectedGenesForNoise.sort());
//     const sitesChanged = !prevSites.current ||
//       JSON.stringify(prevSites.current.sort()) !== JSON.stringify(filterState.selectedSites.sort());
    
//     const genesToUse = filterState.selectedPathway
//     ? filterState.selectedPathway.MatchingGenes
//     : filterState.selectedGenes;
//     const genesSubset = areGenesSubsetOfInitial(genesToUse);

//     // if (genesChanged || sitesChanged) {
//     //   prevGenes.current = selectedGenesForNoise;
//     //   prevSites.current = filterState.selectedSites;
//     //   refetchNoise();
//     // }
//     // fetch **only** if sites changed **or** the genes are *not* a subset of the initial list
//   if (sitesChanged || !genesSubset) {
//     // prevSites.current = sites;
//     prevSites.current = filterState.selectedSites
//     refetchNoise();
//   }
//   }, [selectedGenesForNoise, filterState.selectedSites, refetchNoise]);
  
//   // // === UPDATE FILTERS ===  
//     const updateFilters = useCallback((updates: Partial<FilterState>) => {
//    // dispatch even when arrays are empty (use !== undefined)
//     if (updates.selectedSites !== undefined) dispatch({ type: "SET_SITES", payload: updates.selectedSites });
//     if (updates.selectedGenes !== undefined) dispatch({ type: "SET_GENES", payload: updates.selectedGenes });
//     if (updates.selectedPathway !== undefined) dispatch({ type: "SET_SELECTED_PATHWAY", payload: updates.selectedPathway });
//     if (updates.normalizationMethod !== undefined) dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
//     if (updates.dataFormats) {
//         Object.entries(updates.dataFormats).forEach(([metric, format]) => {
//           if (metric !== "logfc") {
//             dispatch({ type: "SET_DATA_FORMAT", payload: { metric: metric as keyof FilterState["dataFormats"], format: format as "raw" | "log2" } });
//           }
//         });
//       }
//     // update URL params: remove param when empty
//     const newParams: any = { ...Object.fromEntries(searchParams) };
//     if (updates.selectedSites !== undefined) {
//       if (updates.selectedSites && updates.selectedSites.length) newParams.sites = updates.selectedSites.join(",");
//       else delete newParams.sites;
//     }
//     if (updates.selectedGenes !== undefined) {
//       if (updates.selectedGenes && updates.selectedGenes.length) newParams.genes = updates.selectedGenes.join(",");
//       else delete newParams.genes;
//     }
//     if (updates.selectedPathway !== undefined) {
//       if (updates.selectedPathway) newParams.pathway = updates.selectedPathway.Term;
//       else delete newParams.pathway;
//     }
//     if (updates.normalizationMethod !== undefined) {
//       newParams.library = newParams.library ?? params.library;
//       // keep normalization in state only; if you want it in URL add here
//     }
//     setSearchParams(newParams);
//   }, [searchParams, setSearchParams, params.library]);

//     const handleGeneInputSubmit = useCallback(() => {
//     const genes = filterState.geneInput
//       .split(",")
//       .map((g) => g.trim().toUpperCase())
//       .filter((g) => g.length > 0);
//     updateFilters({ selectedGenes: genes, geneInput: genes.join(", "), selectedPathway: null }); // Reset pathway when genes change
//   }, [filterState.geneInput, updateFilters]);

//   const customFilters: FilterSection[] = useMemo(() => {
//     const availableGenes = filterState.selectedPathway
//       ? filterState.selectedPathway.MatchingGenes.map((gene) => ({ id: gene, label: gene }))
//       : [
//           ...new Set(
//             ["raw", "log2"].flatMap((scale) =>
//               ["tpm", "fpkm", "fpkm_uq"].flatMap((method) => Object.keys(resultsData[scale]?.[method]?.gene_stats || {}))
//             )
//           ),
//         ].map((gene) => ({ id: gene, label: gene }));

//     return [
//       {
//         title: "Sites",
//         id: "sites",
//         type: "checkbox",
//         options: availableSites.map((site) => ({
//           id: site.name,
//           label: site.name,
//           tooltip: `Cancer site: ${site.name}`,
//         })),
//         isMasterCheckbox: true,
//         defaultOpen: false,
//       },
//       {
//         title: "Genes",
//         id: "genes",
//         type: "checkbox",
//         options: availableGenes.length > 0 ? availableGenes : params.genes.map((gene) => ({ id: gene, label: gene })),
//         isMasterCheckbox: true,
//         defaultOpen: false,
//       },
//     ];
//   }, [availableSites, resultsData, params.genes, filterState.selectedPathway]);

//   // === PATHWAY TABLE ===
//     const currentResults = resultsData[filterState.dataFormats.mean]?.[filterState.normalizationMethod] || { enrichment: [], gene_stats: {} } as any;
//     const currentEnrichment = currentResults.enrichment || [];
//     const enrichedPathways = useMemo(() => {
//       return (currentEnrichment as any[]).map((p: any, i: number) => {
//         const originalMatchingGenes: string[] =
//       p.MatchingGenes ?? p.matchingGenes ?? p.inputGenes ?? p.inputGenesList ?? p.genes ?? [];
//         // normalize possible API field names and ensure required fields exist
//         const Term: string = p.Term ?? p.term ?? p.value ?? "";
//         const FDR: number = p.FDR ?? p.fdr ?? p.fdr_value ?? 1;
//         const Database: string = p.Database ?? p.database ?? p.source ?? "";
//         const MatchingGenes: string[] = p.MatchingGenes ?? p.matchingGenes ?? p.inputGenes ?? p.inputGenesList ?? p.genes ?? [];
//         const Description: string = p.Description ?? p.description ?? "";
//         const GeneCount: number = p.GeneCount ?? p.gene_count ?? (Array.isArray(MatchingGenes) ? MatchingGenes.length : 0);
//         const EnrichmentScore: number = p.EnrichmentScore ?? p.score ?? 0;
  
//         return {
//           Term,
//           Database,
//           FDR,
//           // MatchingGenes,
//           MatchingGenes: originalMatchingGenes,
//           Description,
//           GeneCount,
//           EnrichmentScore,
//           Rank: i + 1,
//         } as Enrichment & { Rank: number };
//       }).sort((a, b) => a.FDR - b.FDR);
//     }, [currentEnrichment]);

//   const formatFDR = (fdr: number) => fdr.toExponential(2);
//   const pathwayCustomFilters = [
//   {
//     title: "Sites",
//     id: "sites",
//     type: "checkbox" as const,
//     options: availableSites.map(s => ({ id: s.name, label: s.name })),
//     isMasterCheckbox: true,
//     defaultOpen: true,
//   },
//   {
//     title: "Genes",
//     id: "genes",
//     type: "checkbox" as const,
//     options: filterState.selectedPathway
//       ? filterState.selectedPathway.MatchingGenes.map(g => ({ id: g, label: g }))
//       : filterState.selectedGenes.map(g => ({ id: g, label: g })),
//     isMasterCheckbox: true,
//     defaultOpen: true,
//   },
// ];

//   // === RENDER HELPERS ===
//   // const renderValue = (val: number | null) => val === null ? "—" : val.toFixed(3);
//   // Safely render numeric values; treat undefined/null/NaN as em dash
//   const renderValue = (val: number | null | undefined) =>
//   val == null || Number.isNaN(Number(val)) ? "—" : Number(val).toFixed(3);


//   // const norm = filterState.normalizationMethod;
//   // const noiseForNorm = noiseData[norm] || {};

//   // const meanTableData = useMemo(() => {
//   //   return Object.entries(noiseForNorm).map(([gene, sites]) => ({
//   //     gene,
//   //     ...Object.fromEntries(
//   //       filterState.selectedSites.flatMap(site => {
//   //         const s = sites[site.toLowerCase()] as GeneNoise | undefined;
//   //         return [
//   //           [`${site}_normal`, s?.mean_normal ?? null],
//   //           [`${site}_tumor`, s?.mean_tumor ?? null],
//   //         ];
//   //       })
//   //     ),
//   //   }));
//   // }, [noiseForNorm, filterState.selectedSites]);

//   // const cvTableData = useMemo(() => {
//   //   return Object.entries(noiseForNorm).map(([gene, sites]) => ({
//   //     gene,
//   //     ...Object.fromEntries(
//   //       filterState.selectedSites.flatMap(site => {
//   //         const s = sites[site.toLowerCase()] as GeneNoise | undefined;
//   //         return [
//   //           [`${site}_normal`, s?.cv_normal ?? null],
//   //           [`${site}_tumor`, s?.cv_tumor ?? null],
//   //         ];
//   //       })
//   //     ),
//   //   }));
//   // }, [noiseForNorm, filterState.selectedSites]);
//   const norm = filterState.normalizationMethod;
//   // Use pathway-analysis results (has both raw/log2 scales) for table toggles
//   const meanSource = resultsData[filterState.dataFormats.mean]?.[norm]?.gene_stats || {};
//   const cvSource = resultsData[filterState.dataFormats.cv]?.[norm]?.gene_stats || {};
//   const logfcSource = resultsData.log2?.[norm]?.gene_stats || {}; // logfc is provided on log2 scale

//   const meanTableData = useMemo(() => {
//     return Object.entries(meanSource).map(([gene, sites]) => ({
//       gene,
//       ...Object.fromEntries(
//         filterState.selectedSites.flatMap(site => {
//           const s = (sites as any)[site.toLowerCase()] || {};
//           return [
//             [`${site}_normal`, s.mean_normal ?? null],
//             [`${site}_tumor`, s.mean_tumor ?? null],
//           ];
//         })
//       ),
//     }));
//   }, [meanSource, filterState.selectedSites, filterState.dataFormats.mean, norm, resultsData]);

//   const cvTableData = useMemo(() => {
//     return Object.entries(cvSource).map(([gene, sites]) => ({
//       gene,
//       ...Object.fromEntries(
//         filterState.selectedSites.flatMap(site => {
//           const s = (sites as any)[site.toLowerCase()] || {};
//           return [
//             [`${site}_normal`, s.cv_normal ?? null],
//             [`${site}_tumor`, s.cv_tumor ?? null],
//           ];
//         })
//       ),
//     }));
//   }, [cvSource, filterState.selectedSites, filterState.dataFormats.cv, norm, resultsData]);

//   const logfcTableData = useMemo(() => {
//     return Object.entries(logfcSource).map(([gene, sites]) => ({
//       gene,
//     ...Object.fromEntries(
//         filterState.selectedSites.map(site => {
//           const s = (sites as any)[site.toLowerCase()] || {};
//           return [site, s.logfc ?? null];
//         })
//       ),
//     }));
//   }, [logfcSource, filterState.selectedSites, resultsData, norm]);

//   // const logfcTableData = useMemo(() => {
//   //   return Object.entries(noiseForNorm).map(([gene, sites]) => ({
//   //     gene,
//   //     ...Object.fromEntries(
//   //       filterState.selectedSites.map(site => [site, sites[site.toLowerCase()]?.logfc])
//   //     ),
//   //   }));
//   // }, [noiseForNorm, filterState.selectedSites]);
//     const downloadData = useCallback(
//     (type: "enrichment" | "mean_expression" | "noise_metrics") => {
//       const selectedSites = filterState.selectedSites as string[];
//       let data: any[] = [];
//       let headers: string[] = [];
//       let filename = `pathway_analysis_${selectedSites.join("_")}_${filterState.dataFormats.mean}_${filterState.normalizationMethod}`;
//       if (type === "enrichment") {
//         filename = `enrichment_results_${filterState.normalizationMethod}.csv`;
//         headers = ["Rank", "Pathway/GO Term", "Database", "FDR", "Matching Genes", "Description", "Gene Count"];
//         const rows = enrichedPathways.map((row) =>
//           [
//             row.Rank,
//             row.Term,
//             row.Database,
//             formatFDR(row.FDR),
//             row.MatchingGenes.join(" || "),
//             row.Description || "N/A",
//             row.GeneCount || 0,
//           ].join(",")
//         );
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       } else if (type === "mean_expression") {
//         data = Object.entries(currentResults.gene_stats);
//         headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])];
//         filename = `mean_expression_${filename}.csv`;
//         const rows = data.map(([gene, stats]) => {
//           const metrics = selectedSites
//             .map((site) => {
//               const lowerSite = site.toLowerCase();
//               const metric = stats[lowerSite] || {};
//               return [metric.mean_normal?.toFixed(2) || "0.00", metric.mean_tumor?.toFixed(2) || "0.00"];
//             })
//             .flat();
//           return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
//         });
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       } else if (type === "noise_metrics") {
//         data = Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {});
//         headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal CV`, `${site} Tumor CV`, `${site} Log2FC`])];
//         filename = `noise_metrics_${filename}.csv`;
//         const rows = data.map(([gene, stats]) => {
//           const metrics = selectedSites
//             .map((site) => {
//               const lowerSite = site.toLowerCase();
//               const metric = stats[lowerSite] || {};
//               return [
//                 metric.cv_normal?.toFixed(2) || "0.00",
//                 metric.cv_tumor?.toFixed(2) || "0.00",
//                 metric.logfc?.toFixed(2) || "0.00",
//               ];
//             })
//             .flat();
//           return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
//         });
//         const content = [headers.join(","), ...rows].join("\n");
//         const blob = new Blob([content], { type: "text/csv" });
//         const url = URL.createObjectURL(blob);
//         const a = document.createElement("a");
//         a.href = url;
//         a.download = filename;
//         a.click();
//         URL.revokeObjectURL(url);
//       }
//     },
//     [enrichedPathways, filterState.normalizationMethod, resultsData, filterState.dataFormats, filterState.selectedSites]
//   );

//   // === HEATMAP Z-SCORES ===
//   // const getZValues = useCallback((key: "mean" | "cv" | "logfc") => {
//   //   const values: number[] = [];
//   //   Object.entries(noiseForNorm).forEach(([_, sites]) => {
//   //     filterState.selectedSites.forEach(site => {
//   //       const s = (sites as { [k: string]: GeneNoise } | undefined)?.[site.toLowerCase()] as GeneNoise | undefined;
//   //       if (key === "mean") {
//   //         if (s?.mean_normal != null) values.push(s.mean_normal);
//   //         if (s?.mean_tumor != null) values.push(s.mean_tumor);
//   //       } else if (key === "cv") {
//   //         if (s?.cv_normal != null) values.push(s.cv_normal);
//   //         if (s?.cv_tumor != null) values.push(s.cv_tumor);
//   //       } else if (key === "logfc" && s?.logfc != null) {
//   //         values.push(s.logfc);
//   //       }
//   //     });
//   //   });

//   //   const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
//   //   const std = values.length > 1
//   //     ? Math.sqrt(values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (values.length - 1))
//   //     : 0;

//   //   return Object.keys(noiseForNorm).map(gene => {
//   //     const sites = (noiseForNorm as { [gene: string]: { [k: string]: GeneNoise } } | undefined)?.[gene] || {};
//   //     return filterState.selectedSites.flatMap(site => {
//   //       const s = (sites as { [k: string]: GeneNoise })[site.toLowerCase()] as GeneNoise | undefined;
//   //       const z = (v: number | null) => v === null ? null : std > 0 ? (v - mean) / std : 0;
//   //       if (key === "logfc") return [s?.logfc != null ? z(s.logfc) : null];
//   //       return [z(s?.mean_normal ?? null), z(s?.mean_tumor ?? null)];
//   //     });
//   //   });
//   // }, [noiseForNorm, filterState.selectedSites]);

//   const getZValues = useCallback((key: "mean" | "cv" | "logfc") => {
//   const source =
//     key === "mean"
//       ? resultsData[filterState.dataFormats.mean][norm]?.gene_stats
//       : key === "cv"
//       ? resultsData[filterState.dataFormats.cv][norm]?.gene_stats
//       : resultsData.log2[filterState.normalizationMethod]?.gene_stats;

//   if (!source) return [];

//   const values: number[] = [];
//   Object.entries(source).forEach(([_, sites]) => {
//     filterState.selectedSites.forEach(site => {
//       const s = (sites as any)[site.toLowerCase()] || {};
//       if (key === "mean") {
//         if (s.mean_normal != null) values.push(s.mean_normal);
//         if (s.mean_tumor != null) values.push(s.mean_tumor);
//       } else if (key === "cv") {
//         if (s.cv_normal != null) values.push(s.cv_normal);
//         if (s.cv_tumor != null) values.push(s.cv_tumor);
//       } else if (key === "logfc" && s.logfc != null) {
//         values.push(s.logfc);
//       }
//     });
//   });

//   const mean = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
//   const std = values.length > 1
//     ? Math.sqrt(values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / (values.length - 1))
//     : 0;

//   return Object.keys(source).map(gene => {
//     const sites = source[gene] || {};
//     return filterState.selectedSites.flatMap(site => {
//       const s = sites[site.toLowerCase()] || {};
//       const z = (v: number | null) => v == null ? null : std > 0 ? (v - mean) / std : 0;
//       if (key === "logfc") return [s.logfc != null ? z(s.logfc) : null];
//       return [z(s.mean_normal), z(s.mean_tumor)];
//     });
//   });
// }, [
//   resultsData,
//   filterState.dataFormats,
//   filterState.selectedSites,
//   filterState.normalizationMethod,
//   norm, // make sure norm is in scope
// ]);
//   // === WARNING: NO NORMAL SAMPLES ===
//   const hasNoNormal = filterState.selectedSites.some(site => {
//     const c = siteSampleCounts.find(s => s.site === site);
//     return c && c.normal === 0;
//   });

//   return (
//     <div className="min-h-screen bg-white flex flex-col">
//       <Header />
//       <main className="flex-grow">
//         <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
//           <div className="grid grid-cols-[320px_1fr] gap-6">
//             {/* <div> */}
              
//               {/* <FilterPanel
//                 normalizationMethod={filterState.normalizationMethod}
//                 setNormalizationMethod={(v) => updateFilters({ normalizationMethod: v })}
//                 customFilters={[
//                   {
//                     title: "Sites", id: "sites", type: "checkbox",
//                     options: availableSites.map(s => ({ id: s.name, label: s.name })),
//                     isMasterCheckbox: true,
//                   },
//                   {
//                     title: "Genes", id: "genes", type: "checkbox",
//                     options: filterState.selectedPathway
//                       ? filterState.selectedPathway.MatchingGenes.map(g => ({ id: g, label: g }))
//                       : filterState.selectedGenes.map(g => ({ id: g, label: g })),
//                     isMasterCheckbox: true,
//                   },
//                 ]}
//                 onFilterChange={(id, value) => {
//                   if (id === "sites") updateFilters({ selectedSites: value });
//                   if (id === "genes") updateFilters({ selectedGenes: value });
//                 }}
//                 selectedValues={{ sites: filterState.selectedSites, genes: filterState.selectedGenes }}
//               /> */}
//               <FilterPanel
//                 normalizationMethod={filterState.normalizationMethod}
//                 setNormalizationMethod={(value) => updateFilters({ normalizationMethod: value })}
//                 customFilters={customFilters}
//                 onFilterChange={(filterId, value) => {
//                   if (filterId === "sites") {
//                     updateFilters({ selectedSites: value });
//                   } else if (filterId === "genes") {
//                     updateFilters({ selectedGenes: value });
//                   }
//                 }}
//                 selectedValues={{
//                   sites: filterState.selectedSites,
//                   genes: filterState.selectedGenes,
//                 }}
//               />
//               {/* </div> */}

//             <div className="flex-1">
//               {enrichLoading || noiseLoading ? (
//                 <LoadingSpinner message="Loading..." />
//               ) : enrichError ? (
//                 <div className="text-red-600">{enrichError}</div>
//               ) : filterState.selectedSites.length === 0 ? (
//                 <div className="text-red-600">Please select at least one site.</div>
//               ) : (
//                 <>

//                   <Link
//                     to="/pathway-analysis"
//                     className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
//                   >
//                     <ArrowLeft className="h-4 w-4 mr-2" />
//                     Back to Pathway Analysis
//                   </Link>
//                   <div className="mb-8">
//                     <div className="flex items-center justify-between mb-6">
//                       <h2 className="text-4xl font-bold text-blue-900">Results For Pathway Analysis</h2>
//                       <Button onClick={() => downloadData("enrichment")} variant="outline" size="sm">
//                         <Download className="h-4 w-4 mr-2" /> Download Enrichment CSV
//                       </Button>
//                     </div>
//                     <div className="overflow-x-auto mb-6">
//                       <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
//                         <tbody>
//                           {/* <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Analysis Mode</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.analysisMode.toUpperCase()}</td>
//                           </tr> */}
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Normalization</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.normalizationMethod.toUpperCase()}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Genes</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedGenes.join(", ") || "None"}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Selected Pathway</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedPathway?.Description || "None"}</td>
//                           </tr>
//                           <tr>
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Cancer Site(s)</th>
//                             <td className="py-3 px-4 text-blue-700">
//                               {filterState.selectedSites.join(", ")}
//                               {params.cancerTypes.length > 0 && ` (${params.cancerTypes.join(", ")})`}
//                             </td>
//                           </tr>
//                         </tbody>
//                       </table>
//                       {hasNoNormal && (
//                       <Alert className="mb-4">
//                         <AlertDescription>Warning: Some sites have no normal samples. CV-normal and logFC will show "—".
//                     </AlertDescription>
//                       </Alert>
//                     )}
//                     </div>
//                   </div>
//                   <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-green-600 mb-2" />
//                         <div className="text-2xl font-bold text-green-600">{totalNormalSamples}</div>
//                         <div className="text-xs text-gray-600">Total Normal Samples</div>
//                       </CardContent>
//                     </Card>
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-red-600 mb-2" />
//                         <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
//                         <div className="text-xs text-gray-600">Total Tumor Samples</div>
//                       </CardContent>
//                     </Card>
//                   </div>
//                   <SampleCounts
//                     isOpen={filterState.isSampleCountsOpen}
//                     toggleOpen={() => dispatch({ type: "TOGGLE_SAMPLE_COUNTS" })}
//                     siteSampleCounts={siteSampleCounts}
//                     selectedSites={filterState.selectedSites}
//                     selectedGroups={filterState.selectedGroups}
//                   />
//                   {currentResults.enrichment.length === 0 ? (
//                     <Card className="shadow-lg p-6 text-center text-blue-700">
//                       <Activity className="w-10 h-10 mx-auto mb-3" />
//                       <p className="text-lg">No pathway enrichment results found for the selected parameters.</p>
//                     </Card>

//                     ) : (
//                     <>
//                   <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
//                         <div className="lg:col-span-2">
//                            <CollapsibleCard title="Enriched Pathways" className="h-full">
//                              <DataTable
//                               data={enrichedPathways}
//                               columns={[
//                                 { key: "Rank", header: "Rank", sortable: true },
//                                 { key: "Term", header: "Pathway", sortable: true },
//                                 {
//                                   key: "FDR",
//                                   header: "FDR",
//                                   sortable: true,
//                                   render: (value: number) => formatFDR(value),
//                                 },
//                               ]}
//                               scrollHeight="450px"
//                               stickyHeader
//                               defaultSortKey="FDR"
//                               defaultSortOrder="asc"
//                               // onRowClick={(row: Enrichment) =>
//                               //   updateFilters({
//                               //     selectedPathway: filterState.selectedPathway?.Term === row.Term ? null : row,
//                               //     selectedGenes: filterState.selectedPathway?.Term === row.Term ? params.genes : row.MatchingGenes,
//                               //   })
//                               // }
//                               // onRowClick={(row: Enrichment) => {
//                               //  const currentlySelected = filterState.selectedPathway?.Term === row.Term;
//                               //   const newPathway = currentlySelected ? null : row;
//                               //   const newGenes = currentlySelected ? params.genes : row.MatchingGenes;

//                               //   // update reducer state directly
//                               //   dispatch({ type: "SET_SELECTED_PATHWAY", payload: newPathway });
//                               //   dispatch({ type: "SET_GENES", payload: newGenes });

//                               //   // update URL params to reflect selection (remove when cleared)
//                               //   const newParams: any = { ...Object.fromEntries(searchParams) };
//                               //   if (newPathway) {
//                               //     newParams.pathway = newPathway.Term;
//                               //     if (newGenes && newGenes.length) newParams.genes = newGenes.join(",");
//                               //     else delete newParams.genes;
//                               //   } else {
//                               //     delete newParams.pathway;
//                               //     if (params.genes && params.genes.length) newParams.genes = params.genes.join(",");
//                               //     else delete newParams.genes;
//                               //   }
//                               //   setSearchParams(newParams);
//                               //   // fetching is handled by the useEffect that watches selectedPathway / selectedSites
//                               // }}
//                               onRowClick={(row: Enrichment) => {
//                               const currentlySelected = filterState.selectedPathway?.Term === row.Term;
//                               const newPathway = currentlySelected ? null : row;

//                               // Update ONLY the selected pathway — NEVER touch selectedGenes
//                               dispatch({ type: "SET_SELECTED_PATHWAY", payload: newPathway });

//                               // Update URL to reflect current pathway (optional, for bookmarking)
//                               const newParams: any = { ...Object.fromEntries(searchParams) };
//                               if (newPathway) {
//                                 newParams.pathway = newPathway.Term;
//                                 // Do NOT set `genes` here — keep original genes in URL
//                               } else {
//                                 delete newParams.pathway;
//                               }
//                               setSearchParams(newParams);

//                               // Trigger enrichment refetch (if needed) — safe because sites didn't change
//                               // Noise fetch will use: selectedPathway ? MatchingGenes : selectedGenes
//                               // → useEffect below will handle it correctly
//                             }}
//                               containerWidth="565px"
//                               rowClassName={(row) => {
//                                 const isSelected = filterState.selectedPathway?.Term === row.Term;
//                                 return isSelected ? "bg-blue-100 font-semibold" : "";
//                               }}
//                             />
//                           </CollapsibleCard>
//                         </div>
//                         <div>
//                           <CollapsibleCard title="Pathway Details" className="h-full">
//                             {filterState.selectedPathway ? (
//                               <div className="space-y-2 p-2">
//                                 <div>
//                                   <h3 className="font-semibold text-blue-700 text-sm">{filterState.selectedPathway.Term}</h3>
//                                   <p className="text-xs text-gray-600">Database: {filterState.selectedPathway.Database}</p>
//                                   <p className="text-xs text-gray-600">Description: {filterState.selectedPathway.Description || "N/A"}</p>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Statistics</h4>
//                                   <div className="grid grid-cols-2 gap-1 mt-1 text-xs">
//                                     <div>FDR:</div>
//                                     <div>{formatFDR(filterState.selectedPathway.FDR)}</div>
//                                     <div>Gene Count:</div>
//                                     <div>{filterState.selectedPathway.GeneCount}</div>
//                                     {/* <div>Enrichment Score:</div>
//                                     <div>{filterState.selectedPathway.EnrichmentScore?.toFixed(2) || "N/A"}</div> */}
//                                   </div>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Matching Genes</h4>
//                                   <div className="flex flex-wrap gap-1 mt-1">
//                                     {filterState.selectedPathway.MatchingGenes.map((gene, idx) => (
//                                       <span key={idx} className="bg-blue-100 text-blue-800 px-1 py-0.5 rounded text-xs">
//                                         {gene}
//                                       </span>
//                                     ))}
//                                   </div>
//                                 </div>
//                               </div>
//                             ) : (
//                               <div className="text-center text-gray-500 h-full flex items-center justify-center">
//                                 <div>
//                                   <Info className="h-6 w-6 mx-auto mb-1" />
//                                   <p className="text-xs">Select a pathway to view details</p>
//                                 </div>
//                               </div>
//                             )}
//                           </CollapsibleCard>
//                         </div>
//                       </div>

//                   {filterState.selectedPathway && (
//                     <>
//                     <CollapsibleCard title="Mean Expression" className="mb-4">
//                         <PlotlyHeatmap
//                               title="Mean Expression (Z-scores)"
//                               data={Object.entries(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("mean")}
//                               zLabel={`Mean Expression (${filterState.dataFormats.mean.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="expression-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                               // colorscale="Pastel1"
//                             />
//                           </CollapsibleCard>
//                           <CollapsibleCard title="Coefficient of Variation (CV)" className="mb-4">

//                       <PlotlyHeatmap
//                               title="Coefficient of Variation (Z-scores)"
//                               data={Object.entries(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("cv")}
//                               zLabel={`Noise (${filterState.dataFormats.cv.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="cv-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                               // colorscale="Mint"
//                             />
//                           </CollapsibleCard>
//                           <CollapsibleCard title="Differential Noise" className="mb-4">
//                             <PlotlyHeatmap
//                               title={`Log₂(CV<sub>tumor</sub> / CV<sub>normal</sub>)`}
//                               data={Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites}
//                               yValues={Object.keys(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("logfc")}
//                               zLabel="Log₂FC"
//                               chartKey="logfc-heatmap"
//                               xLabel="Cancer Sites"
//                               yLabel="Genes"
//                               // colorscale="Mint"
//                             />
//                           </CollapsibleCard>
//                       <CollapsibleCard
//                             title="Mean Expression"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("mean_expression")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="mean-table-log2-switch"
//                                 checked={filterState.dataFormats.mean === "log2"}
//                                 onCheckedChange={(checked) => {
//                                 dispatch({
//                                  type: "SET_DATA_FORMAT",
//                                   payload: { metric: "mean", format: checked ? "log2" : "raw" },
//                                 });
//                                   console.debug("SET_DATA_FORMAT mean ->", checked ? "log2" : "raw");
//                                 }}
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="mean-table-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                         <DataTable
//                           data={meanTableData}
//                           columns={[
//                             { key: "gene", header: "Gene", sortable: true,},
//                             ...filterState.selectedSites.flatMap(site => [
//                               { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true, },
//                               { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true, },
//                             ]),
//                           ]}
//                           containerWidth="850px"   // any fixed width you like
//                           scrollHeight="450px"
//                         />
//                       </CollapsibleCard>
//                       <CollapsibleCard
//                             title="Gene Noise (CV)"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="cv-table-log2-switch"
//                                 checked={filterState.dataFormats.cv === "log2"}
//                               //   onCheckedChange={(checked) =>
//                               //     updateFilters({ dataFormats: { ...filterState.dataFormats, cv: checked ? "log2" : "raw" } })
//                               //   }
//                               //   className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               // />
//                               onCheckedChange={(checked) => {
//                                 dispatch({
//                                  type: "SET_DATA_FORMAT",
//                                   payload: { metric: "cv", format: checked ? "log2" : "raw" },
//                                 });
//                                   console.debug("SET_DATA_FORMAT cv ->", checked ? "log2" : "raw");
//                                 }}
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="cv-table-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                         <DataTable
//                           data={cvTableData}
//                           columns={[
//                             { key: "gene", header: "Gene", sortable: true, },
//                             ...filterState.selectedSites.flatMap(site => [
//                               { key: `${site}_normal`, header: `${site} Normal`, render: (_, r) => renderValue(r[`${site}_normal`]), sortable: true, },
//                               { key: `${site}_tumor`, header: `${site} Tumor`, render: (_, r) => renderValue(r[`${site}_tumor`]), sortable: true, },
//                             ]),
//                           ]}
//                           containerWidth="850px"   // any fixed width you like
//                           scrollHeight="450px"
//                         />
//                       </CollapsibleCard>

//                       <CollapsibleCard title="Differential Noise (LogFC)"
//                       className="mb-4"
//                       downloadButton={
//                               <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                       >
//                         <DataTable
//                           data={logfcTableData}
//                           columns={[
//                             { key: "gene", header: "Gene", sortable: true, },
//                             ...filterState.selectedSites.map(site => ({
//                               key: site,
//                               header: site,
//                               render: (_, r) => renderValue(r[site]),
//                               sortable: true,
//                             })),
//                           ]}
//                           containerWidth="850px"   // any fixed width you like
//                           scrollHeight="450px"
//                         />
//                       </CollapsibleCard>

                      
//                     </>
//                   )}
//                 </>
//               )}
//             </>
//               )}
//             </div>
          
//           </div>
//         </div>
//       </main>
//       <Footer />
//     </div>
//   );
// };

// export default React.memo(PathwayResults);


// import React, { useMemo, useCallback, useReducer, useEffect, useRef, useState } from "react";
// import { useSearchParams, Link } from "react-router-dom";
// import { Card, CardContent } from "@/components/ui/card";
// import { Button } from "@/components/ui/button";
// import { ArrowLeft, Download, Activity, Users, Info } from "lucide-react";
// import { PlotlyHeatmap } from "@/components/charts/index";
// import Header from "@/components/header";
// import Footer from "@/components/footer";
// import { useCache } from "@/hooks/use-cache";
// import { LoadingSpinner } from "@/components/ui/loadingSpinner";
// import FilterPanel from "@/components/FilterPanel";
// import { DataTable } from "@/components/ui/data-table";
// import CollapsibleCard from "@/components/ui/collapsible-card";
// import { Switch } from "@/components/ui/switch";
// import { Label } from "@/components/ui/label";
// import SampleCounts from "@/components/SampleCounts";

// // Interfaces
// export interface GeneMetrics {
//   mean_tumor: number;
//   mean_normal: number;
//   cv_tumor: number;
//   cv_normal: number;
//   logfc: number;
// }

// export interface GeneStats {
//   [cancer: string]: GeneMetrics;
// }

// export interface Enrichment {
//   Term: string;
//   Database: string;
//   FDR: number;
//   MatchingGenes: string[];
//   Description?: string;
//   GeneCount?: number;
//   EnrichmentScore?: number;
//   GeneSet?: string;
// }

// export interface HeatmapData {
//   [gene: string]: {
//     [key: string]: number;
//   };
// }

// export interface NetworkEdge {
//   source: string;
//   target: string;
//   score: number;
// }

// export interface NormalizationResults {
//   enrichment: Enrichment[];
//   network?: NetworkEdge[];
//   top_genes: string[];
//   gene_stats: { [gene: string]: GeneStats };
//   heatmap_data: HeatmapData;
//   noise_metrics: { [key: string]: any };
//   warning?: string | null;
// }

// export interface ResultsData {
//   raw: {
//     [method: string]: NormalizationResults;
//   };
//   log2: {
//     [method: string]: NormalizationResults;
//   };
// }

// interface FilterOption {
//   id: string;
//   label: string;
//   tooltip?: string;
// }

// interface FilterSection {
//   title: string;
//   id: string;
//   options: FilterOption[];
//   isMasterCheckbox?: boolean;
//   type: "checkbox" | "radio";
//   defaultOpen?: boolean;
// }

// // Filter State and Reducer
// interface FilterState {
//   selectedGroups: string[];
//   selectedSites: string[];
//   selectedGenes: string[];
//   normalizationMethod: string;
//   dataFormats: { mean: "raw" | "log2"; cv: "raw" | "log2"; logfc: "log2" };
//   selectedPathway: Enrichment | null;
//   sortBy: "fdr";
//   sortOrder: "asc" | "desc";
//   logFCThreshold: number;
//   isSampleCountsOpen: boolean;
//   analysisMode: "enrichment" | "neighbors";
//   geneInput: string;
//   showNetwork: boolean;
// }

// type FilterAction =
//   | { type: "SET_GROUPS"; payload: string[] }
//   | { type: "SET_SITES"; payload: string[] }
//   | { type: "SET_GENES"; payload: string[] }
//   | { type: "SET_NORMALIZATION"; payload: string }
//   | { type: "SET_DATA_FORMAT"; payload: { metric: keyof FilterState["dataFormats"]; format: "raw" | "log2" } }
//   | { type: "SET_SELECTED_PATHWAY"; payload: Enrichment | null }
//   | { type: "SET_SORT_BY"; payload: "fdr" }
//   | { type: "SET_SORT_ORDER"; payload: "asc" | "desc" }
//   | { type: "SET_LOGFC_THRESHOLD"; payload: number }
//   | { type: "TOGGLE_SAMPLE_COUNTS" }
//   | { type: "SET_ANALYSIS_MODE"; payload: "enrichment" | "neighbors" }
//   | { type: "SET_GENE_INPUT"; payload: string }
//   | { type: "TOGGLE_NETWORK" };

// const initialFilterState: FilterState = {
//   selectedGroups: ["normal", "tumor"],
//   selectedSites: [],
//   selectedGenes: [],
//   normalizationMethod: "tpm",
//   dataFormats: { mean: "raw", cv: "raw", logfc: "log2" },
//   selectedPathway: null,
//   sortBy: "fdr",
//   sortOrder: "asc",
//   logFCThreshold: 0.7,
//   isSampleCountsOpen: true,
//   analysisMode: "enrichment", // Changed default to "enrichment"
//   geneInput: "",
//   showNetwork: false,
// };

// const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
//   switch (action.type) {
//     case "SET_GROUPS":
//       return { ...state, selectedGroups: action.payload };
//     case "SET_SITES":
//       return { ...state, selectedSites: action.payload };
//     case "SET_GENES":
//       return { ...state, selectedGenes: action.payload };
//     case "SET_NORMALIZATION":
//       return { ...state, normalizationMethod: action.payload };
//     case "SET_DATA_FORMAT":
//       if (action.payload.metric === "logfc") return state;
//       return { ...state, dataFormats: { ...state.dataFormats, [action.payload.metric]: action.payload.format } };
//     case "SET_SELECTED_PATHWAY":
//       return { ...state, selectedPathway: action.payload };
//     case "SET_SORT_BY":
//       return { ...state, sortBy: action.payload };
//     case "SET_SORT_ORDER":
//       return { ...state, sortOrder: action.payload };
//     case "SET_LOGFC_THRESHOLD":
//       return { ...state, logFCThreshold: action.payload };
//     case "TOGGLE_SAMPLE_COUNTS":
//       return { ...state, isSampleCountsOpen: !state.isSampleCountsOpen };
//     case "SET_ANALYSIS_MODE":
//       return { ...state, analysisMode: action.payload };
//     case "SET_GENE_INPUT":
//       return { ...state, geneInput: action.payload };
//     case "TOGGLE_NETWORK":
//       return { ...state, showNetwork: !state.showNetwork };
//     default:
//       return state;
//   }
// };

// // Custom Data Hook
// interface PathwayResultsState {
//   resultsData: ResultsData;
//   isLoading: boolean;
//   error: string | null;
//   totalTumorSamples: number;
//   totalNormalSamples: number;
//   siteSampleCounts: { site: string; tumor: number; normal: number }[];
//   availableSites: { id: number; name: string }[];
//   fetchedSites: string[];
// }

// const usePathwayResultsData = (
//   params: {
//     sites: string[];
//     cancerTypes: string[];
//     library: string;
//     genes: string[];
//     topN: number;
//     mode: string;
//     pathway?: string; // Added pathway to params
//   },
//   filterState: FilterState
// ) => {
//   const [state, setState] = useState<PathwayResultsState>({
//     resultsData: { raw: {}, log2: {} },
//     isLoading: false,
//     error: null,
//     totalTumorSamples: 0,
//     totalNormalSamples: 0,
//     siteSampleCounts: [],
//     availableSites: [],
//     fetchedSites: [],
//   });

//   // const { getCachedData, setCachedData, generateCacheKey } = useCache<
//   //   ResultsData & {
//   //     sample_counts: { [site: string]: { tumor: number; normal: number } };
//   //     available_sites: { id: number; name: string }[];
//   //   }
//   // >();
//   const { getCachedData, setCachedData, generateCacheKey } = useCache<ResultsData & {
//     sample_counts: { [site: string]: { tumor: number; normal: number } };
//     available_sites: { id: number; name: string }[];
//   }>();

//   const filterTimeoutRef = useRef<NodeJS.Timeout | null>(null);
//   const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"];

//   const mergeResultsData = useCallback((prev: ResultsData, newData: ResultsData): ResultsData => {
//     const merged: ResultsData = { raw: { ...prev.raw }, log2: { ...prev.log2 } };
//     normalizationMethods.forEach((norm) => {
//       ["raw", "log2"].forEach((format) => {
//         const existing = merged[format][norm] || {
//           enrichment: [],
//           network: [],
//           top_genes: [],
//           gene_stats: {},
//           heatmap_data: {},
//           noise_metrics: {},
//         };
//         const newDataSection = newData[format]?.[norm] || {
//           enrichment: [],
//           network: [],
//           top_genes: [],
//           gene_stats: {},
//           heatmap_data: {},
//           noise_metrics: {},
//         };
//         merged[format][norm] = {
//           enrichment: existing.enrichment.length > 0 ? existing.enrichment : newDataSection.enrichment,
//           network: existing.network?.length > 0 ? existing.network : newDataSection.network,
//           top_genes: existing.top_genes.length > 0 ? existing.top_genes : newDataSection.top_genes,
//           gene_stats: {
//             ...existing.gene_stats,
//             ...Object.keys(newDataSection.gene_stats).reduce((acc, gene) => {
//               if (!existing.gene_stats?.[gene]) {
//                 acc[gene] = newDataSection.gene_stats[gene];
//               } else {
//                 acc[gene] = {
//                   ...existing.gene_stats[gene],
//                   ...newDataSection.gene_stats[gene],
//                 };
//               }
//               return acc;
//             }, {} as any),
//           },
//           heatmap_data: {
//             ...existing.heatmap_data,
//             ...newDataSection.heatmap_data,
//           },
//           noise_metrics: {
//             ...existing.noise_metrics,
//             ...newDataSection.noise_metrics,
//           },
//         };
//       });
//     });
//     return merged;
//   }, []);

//   const mergeSampleCounts = useCallback(
//     (
//       prevCounts: { [site: string]: { tumor: number; normal: number } },
//       newCounts: { [site: string]: { tumor: number; normal: number } }
//     ) => {
//       const merged: { [site: string]: { tumor: number; normal: number } } = { ...prevCounts };
//       Object.entries(newCounts).forEach(([site, counts]) => {
//         if (!merged[site]) {
//           merged[site] = counts;
//         }
//       });
//       return merged;
//     },
//     []
//   );

//   const fetchData = useCallback(
//     async (sitesToFetch: string[], normalizationMethods: string[] = ["tpm", "fpkm", "fpkm_uq"]) => {
//       if (!sitesToFetch.length || (!filterState.selectedGenes.length && filterState.analysisMode === "enrichment")) {
//         setState((prev) => ({
//           ...prev,
//           error: "Please select at least one cancer site and, for enrichment analysis, at least one gene.",
//           isLoading: false,
//         }));
//         return;
//       }

//       const batchCacheKey = generateCacheKey({
//         sites: sitesToFetch,
//         cancerTypes: params.cancerTypes,
//         library: params.library,
//         topN: params.topN,
//         normalizationMethod: filterState.normalizationMethod,
//         dataFormat: filterState.dataFormats.mean,
//         mode: filterState.analysisMode,
//         pathway: params.pathway, // Include pathway in cache key
//       });

//       const cachedBatchData = getCachedData(batchCacheKey);
//       if (cachedBatchData) {
//         setState((prev) => ({
//           ...prev,
//           resultsData: mergeResultsData(prev.resultsData, cachedBatchData),
//           fetchedSites: [...new Set([...prev.fetchedSites, ...sitesToFetch])],
//           availableSites: cachedBatchData.available_sites || prev.availableSites,
//           siteSampleCounts: Object.entries(cachedBatchData.sample_counts || {}).map(([site, counts]) => ({
//             site,
//             tumor: counts.tumor,
//             normal: counts.normal,
//           })),
//           totalTumorSamples: Object.values(cachedBatchData.sample_counts || {}).reduce((sum, c) => sum + c.tumor, 0),
//           totalNormalSamples: Object.values(cachedBatchData.sample_counts || {}).reduce((sum, c) => sum + c.normal, 0),
//           error: null,
//           isLoading: false,
//         }));
//         return;
//       }

//       const resultsBySite: { [site: string]: ResultsData } = {};
//       const sampleCountsBySite: { [site: string]: { tumor: number; normal: number } } = {};
//       const availableSitesBySite: { [site: string]: { id: number; name: string }[] } = {};
//       const sitesToFetchFromAPI: string[] = [];

//       for (const site of sitesToFetch) {
//         const individualCacheKey = generateCacheKey({
//           sites: [site],
//           cancerTypes: params.cancerTypes,
//           library: params.library,
//           topN: params.topN,
//           normalizationMethod: filterState.normalizationMethod,
//           dataFormat: filterState.dataFormats.mean,
//           mode: filterState.analysisMode,
//           pathway: params.pathway, // Include pathway in individual cache key
//         });
//         const cachedSiteData = getCachedData(individualCacheKey);
//         if (cachedSiteData) {
//           resultsBySite[site] = cachedSiteData;
//           if (cachedSiteData.sample_counts) {
//             sampleCountsBySite[site] = cachedSiteData.sample_counts[site] || { tumor: 0, normal: 0 };
//           }
//           if (cachedSiteData.available_sites) {
//             availableSitesBySite[site] = cachedSiteData.available_sites;
//           }
//         } else {
//           sitesToFetchFromAPI.push(site);
//         }
//       }

//       if (Object.keys(resultsBySite).length > 0) {
//         setState((prev) => {
//           let mergedData = prev.resultsData;
//           let mergedSampleCounts: { [site: string]: { tumor: number; normal: number } } = {};
//           let mergedAvailableSites: { id: number; name: string }[] = prev.availableSites;

//           Object.values(resultsBySite).forEach((siteData) => {
//             mergedData = mergeResultsData(mergedData, siteData);
//           });

//           Object.entries(sampleCountsBySite).forEach(([site, counts]) => {
//             mergedSampleCounts = mergeSampleCounts(mergedSampleCounts, { [site]: counts });
//           });

//           const allAvailableSites = Object.values(availableSitesBySite).flat();
//           if (allAvailableSites.length > 0) {
//             mergedAvailableSites = allAvailableSites;
//           }

//           return {
//             ...prev,
//             resultsData: mergedData,
//             fetchedSites: [...new Set([...prev.fetchedSites, ...Object.keys(resultsBySite)])],
//             availableSites: mergedAvailableSites,
//             siteSampleCounts: Object.entries(mergedSampleCounts).map(([site, counts]) => ({
//               site,
//               tumor: counts.tumor,
//               normal: counts.normal,
//             })),
//             totalTumorSamples: Object.values(mergedSampleCounts).reduce((sum, c) => sum + c.tumor, 0),
//             totalNormalSamples: Object.values(mergedSampleCounts).reduce((sum, c) => sum + c.normal, 0),
//             error: null,
//             isLoading: sitesToFetchFromAPI.length > 0,
//           };
//         });
//       }

//       if (sitesToFetchFromAPI.length === 0) {
//         return;
//       }

//       setState((prev) => ({ ...prev, isLoading: true, error: null }));
//       try {
//         const cancerParam = sitesToFetchFromAPI.join(",");
//         const queryParams = new URLSearchParams({
//           cancer: cancerParam,
//           top_n: params.topN.toString(),
//           library: params.library,
//           mode: filterState.analysisMode,
//           ...(filterState.selectedGenes.length > 0 && {
//             genes: filterState.selectedGenes.join(","),
//           }),
//           ...(params.pathway && { pathway: params.pathway }), // Include pathway in API query
//         });
//         const response = await fetch(`/api/pathway-analysis?${queryParams}`);
//         if (!response.ok) {
//           const errorText = await response.text();
//           throw new Error(`Failed to fetch pathway analysis data: ${errorText}`);
//         }
//         const apiData: ResultsData & {
//           sample_counts: { [site: string]: { tumor: number; normal: number } };
//           available_sites: { id: number; name: string }[];
//         } = await response.json();
//         console.log(apiData);
//         setCachedData(batchCacheKey, apiData);

//         setState((prev) => {
//           const mergedData = mergeResultsData(prev.resultsData, apiData);
//           const mergedSampleCounts = mergeSampleCounts(
//             Object.fromEntries(prev.siteSampleCounts.map((c) => [c.site, { tumor: c.tumor, normal: c.normal }])),
//             apiData.sample_counts || {}
//           );

//           return {
//             ...prev,
//             resultsData: mergedData,
//             fetchedSites: [...new Set([...prev.fetchedSites, ...sitesToFetchFromAPI])],
//             availableSites: apiData.available_sites || prev.availableSites,
//             siteSampleCounts: Object.entries(mergedSampleCounts).map(([site, counts]) => ({
//               site,
//               tumor: counts.tumor,
//               normal: counts.normal,
//             })),
//             totalTumorSamples: Object.values(mergedSampleCounts).reduce((sum, c) => sum + c.tumor, 0),
//             totalNormalSamples: Object.values(mergedSampleCounts).reduce((sum, c) => sum + c.normal, 0),
//             error: null,
//             isLoading: false,
//           };
//         });
//       } catch (error: any) {
//         console.error("Error fetching data:", error);
//         setState((prev) => ({
//           ...prev,
//           error: error.message || "An error occurred while fetching data.",
//           isLoading: false,
//         }));
//       }
//     },
//     [
//       params,
//       filterState.selectedGenes,
//       filterState.normalizationMethod,
//       filterState.dataFormats.mean,
//       filterState.analysisMode,
//       getCachedData,
//       setCachedData,
//       generateCacheKey,
//       mergeResultsData,
//       mergeSampleCounts,
//     ]
//   );

//   const debouncedFetchData = useCallback(
//     (sitesToFetch: string[], normalizationMethods: string[] = ["tpm", "fpkm", "fpkm_uq"]) => {
//       if (filterTimeoutRef.current) {
//         clearTimeout(filterTimeoutRef.current);
//       }
//       filterTimeoutRef.current = setTimeout(() => {
//         fetchData(sitesToFetch, normalizationMethods);
//       }, 500);
//     },
//     [fetchData]
//   );

//   useEffect(() => {
//     const newSites = filterState.selectedSites.filter((site) => !state.fetchedSites.includes(site));
//     if (newSites.length > 0 && (filterState.selectedGenes.length > 0 || filterState.analysisMode === "neighbors")) {
//       debouncedFetchData(newSites, normalizationMethods);
//     }
//   }, [filterState.selectedSites, filterState.selectedGenes, filterState.analysisMode, params.cancerTypes, debouncedFetchData]);

//   return { ...state, fetchData: debouncedFetchData };
// };

// // // Main Component
// // const PathwayResults: React.FC = () => {
// //   const [searchParams, setSearchParams] = useSearchParams();
// //   const { getCachedData } = useCache<any>();

// //   const params = useMemo(
// //     () => ({
// //       sites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
// //       cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
// //       library: searchParams.get("library") || "KEGG_2021_Human",
// //       genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
// //       topN: parseInt(searchParams.get("top_n") || "100", 10),
// //       mode: searchParams.get("mode") || "enrichment",
// //       pathway: searchParams.get("pathway"), // Added pathway to params
// //     }),
// //     [searchParams]
// //   );

// //   useEffect(() => {
// //     window.scrollTo(0, 0);
// //   }, []);

// //   const [filterState, dispatch] = useReducer(filterReducer, {
// //     ...initialFilterState,
// //     selectedSites: params.sites,
// //     selectedGenes: params.genes,
// //     analysisMode: params.mode as "enrichment" | "neighbors",
// //     geneInput: params.genes.join(", "),
// //   });

// //   const { resultsData, isLoading, error, totalTumorSamples, totalNormalSamples, siteSampleCounts, availableSites, fetchData } =
// //     usePathwayResultsData(params, filterState);

// //   // Initialize selectedPathway from params or cache
// //   useEffect(() => {
// //     if (params.pathway && !filterState.selectedPathway) {
// //       const cacheKey = generateCacheKey({
// //         sites: params.sites,
// //         cancerTypes: params.cancerTypes,
// //         genes: params.genes,
// //         analysisType: "ORA",
// //         siteAnalysisType: "pan-cancer",
// //         pathway: params.pathway,
// //       });
// //       const cachedData = getCachedData(cacheKey);
// //       const pathwayFromCache = cachedData?.pathway;

// //       const pathwayFromResults = resultsData.raw[filterState.normalizationMethod]?.enrichment.find(
// //         (p) => p.Term === params.pathway
// //       );

// //       if (pathwayFromResults) {
// //         dispatch({ type: "SET_SELECTED_PATHWAY", payload: pathwayFromResults });
// //         // Update selectedGenes to matching genes of the pathway
// //         dispatch({ type: "SET_GENES", payload: pathwayFromResults.MatchingGenes });
// //       } else if (pathwayFromCache) {
// //         dispatch({ type: "SET_SELECTED_PATHWAY", payload: pathwayFromCache });
// //         dispatch({ type: "SET_GENES", payload: pathwayFromCache.MatchingGenes });
// //       }
// //     }
// //   }, [params.pathway, resultsData, filterState.normalizationMethod, getCachedData]);

// const PathwayResults: React.FC = () => {
//   const [searchParams, setSearchParams] = useSearchParams();
//   const { getCachedData, generateCacheKey } = useCache<any>(); // Destructure generateCacheKey here too

//   const params = useMemo(
//     () => ({
//       sites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
//       cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
//       library: searchParams.get("library") || "KEGG_2021_Human",
//       genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
//       topN: parseInt(searchParams.get("top_n") || "100", 10),
//       mode: searchParams.get("mode") || "enrichment",
//       pathway: searchParams.get("pathway"),
//     }),
//     [searchParams]
//   );

//   useEffect(() => {
//     window.scrollTo(0, 0);
//   }, []);

//   const [filterState, dispatch] = useReducer(filterReducer, {
//     ...initialFilterState,
//     selectedSites: params.sites,
//     selectedGenes: params.genes,
//     analysisMode: params.mode as "enrichment" | "neighbors",
//     geneInput: params.genes.join(", "),
//   });

//   const { resultsData, isLoading, error, totalTumorSamples, totalNormalSamples, siteSampleCounts, availableSites, fetchData } =
//     usePathwayResultsData(params, filterState);

//   // Initialize selectedPathway from params or cache
//   useEffect(() => {
//     if (params.pathway && !filterState.selectedPathway) {
//       const cacheKey = generateCacheKey({
//         sites: params.sites,
//         cancerTypes: params.cancerTypes,
//         genes: params.genes,
//         analysisType: "ORA",
//         siteAnalysisType: "pan-cancer",
//         pathway: params.pathway,
//       });
//       const cachedData = getCachedData(cacheKey);
//       const pathwayFromCache = cachedData?.pathway;

//       const pathwayFromResults = resultsData.raw[filterState.normalizationMethod]?.enrichment.find(
//         (p) => p.Term === params.pathway
//       );

//       if (pathwayFromResults) {
//         dispatch({ type: "SET_SELECTED_PATHWAY", payload: pathwayFromResults });
//         dispatch({ type: "SET_GENES", payload: pathwayFromResults.MatchingGenes });
//       } else if (pathwayFromCache) {
//         dispatch({ type: "SET_SELECTED_PATHWAY", payload: pathwayFromCache });
//         dispatch({ type: "SET_GENES", payload: pathwayFromCache.MatchingGenes });
//       }
//     }
//   }, [params.pathway, resultsData, filterState.normalizationMethod, getCachedData, generateCacheKey]);

//   const updateFilters = useCallback(
//     (updates: Partial<FilterState>) => {
//       if (updates.selectedGroups) dispatch({ type: "SET_GROUPS", payload: updates.selectedGroups });
//       if (updates.selectedSites) dispatch({ type: "SET_SITES", payload: updates.selectedSites });
//       if (updates.selectedGenes) dispatch({ type: "SET_GENES", payload: updates.selectedGenes });
//       if (updates.normalizationMethod) dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
//       if (updates.dataFormats) {
//         Object.entries(updates.dataFormats).forEach(([metric, format]) => {
//           if (metric !== "logfc") {
//             dispatch({ type: "SET_DATA_FORMAT", payload: { metric: metric as keyof FilterState["dataFormats"], format: format as "raw" | "log2" } });
//           }
//         });
//       }
//       if (updates.selectedPathway !== undefined) dispatch({ type: "SET_SELECTED_PATHWAY", payload: updates.selectedPathway });
//       if (updates.sortBy) dispatch({ type: "SET_SORT_BY", payload: updates.sortBy });
//       if (updates.sortOrder) dispatch({ type: "SET_SORT_ORDER", payload: updates.sortOrder });
//       if (updates.logFCThreshold) dispatch({ type: "SET_LOGFC_THRESHOLD", payload: updates.logFCThreshold });
//       if (updates.analysisMode) dispatch({ type: "SET_ANALYSIS_MODE", payload: updates.analysisMode });
//       if (updates.geneInput !== undefined) dispatch({ type: "SET_GENE_INPUT", payload: updates.geneInput });

//       const newParams = { ...Object.fromEntries(searchParams) };
//       if (updates.selectedSites) newParams.sites = updates.selectedSites.join(",");
//       if (updates.selectedGenes) newParams.genes = updates.selectedGenes.join(",");
//       if (updates.analysisMode) newParams.mode = updates.analysisMode;
//       if (updates.selectedPathway !== undefined) newParams.pathway = updates.selectedPathway?.Term || "";
//       setSearchParams(newParams);
//     },
//     [setSearchParams, searchParams]
//   );

  // const handleGeneInputSubmit = useCallback(() => {
  //   const genes = filterState.geneInput
  //     .split(",")
  //     .map((g) => g.trim().toUpperCase())
  //     .filter((g) => g.length > 0);
  //   updateFilters({ selectedGenes: genes, geneInput: genes.join(", "), selectedPathway: null }); // Reset pathway when genes change
  // }, [filterState.geneInput, updateFilters]);

//   const currentResults = useMemo(() => {
//     return (
//       resultsData[filterState.dataFormats.mean][filterState.normalizationMethod] || {
//         enrichment: [],
//         network: [],
//         top_genes: [],
//         gene_stats: {},
//         heatmap_data: {},
//         noise_metrics: {},
//         warning: null,
//       }
//     );
//   }, [resultsData, filterState.dataFormats.mean, filterState.normalizationMethod]);

  // const customFilters: FilterSection[] = useMemo(() => {
  //   const availableGenes = filterState.selectedPathway
  //     ? filterState.selectedPathway.MatchingGenes.map((gene) => ({ id: gene, label: gene }))
  //     : [
  //         ...new Set(
  //           ["raw", "log2"].flatMap((scale) =>
  //             ["tpm", "fpkm", "fpkm_uq"].flatMap((method) => Object.keys(resultsData[scale]?.[method]?.gene_stats || {}))
  //           )
  //         ),
  //       ].map((gene) => ({ id: gene, label: gene }));

  //   return [
  //     {
  //       title: "Sites",
  //       id: "sites",
  //       type: "checkbox",
  //       options: availableSites.map((site) => ({
  //         id: site.name,
  //         label: site.name,
  //         tooltip: `Cancer site: ${site.name}`,
  //       })),
  //       isMasterCheckbox: true,
  //       defaultOpen: false,
  //     },
  //     {
  //       title: "Genes",
  //       id: "genes",
  //       type: "checkbox",
  //       options: availableGenes.length > 0 ? availableGenes : params.genes.map((gene) => ({ id: gene, label: gene })),
  //       isMasterCheckbox: true,
  //       defaultOpen: false,
  //     },
  //   ];
  // }, [availableSites, resultsData, params.genes, filterState.selectedPathway]);

//   const enrichedPathways = useMemo(() => {
//     return currentResults.enrichment
//       .map((pathway, index) => ({
//         ...pathway,
//         Rank: index + 1,
//       }))
//       .sort((a, b) => a.FDR - b.FDR);
//   }, [currentResults.enrichment]);

//   const formatFDR = (fdr: number) => fdr.toExponential(2);

  // const downloadData = useCallback(
  //   (type: "enrichment" | "mean_expression" | "noise_metrics") => {
  //     const selectedSites = filterState.selectedSites as string[];
  //     let data: any[] = [];
  //     let headers: string[] = [];
  //     let filename = `pathway_analysis_${selectedSites.join("_")}_${filterState.dataFormats.mean}_${filterState.normalizationMethod}`;
  //     if (type === "enrichment") {
  //       filename = `enrichment_results_${filterState.normalizationMethod}.csv`;
  //       headers = ["Rank", "Pathway/GO Term", "Database", "FDR", "Matching Genes", "Description", "Gene Count"];
  //       const rows = enrichedPathways.map((row) =>
  //         [
  //           row.Rank,
  //           row.Term,
  //           row.Database,
  //           formatFDR(row.FDR),
  //           row.MatchingGenes.join(" || "),
  //           row.Description || "N/A",
  //           row.GeneCount || 0,
  //         ].join(",")
  //       );
  //       const content = [headers.join(","), ...rows].join("\n");
  //       const blob = new Blob([content], { type: "text/csv" });
  //       const url = URL.createObjectURL(blob);
  //       const a = document.createElement("a");
  //       a.href = url;
  //       a.download = filename;
  //       a.click();
  //       URL.revokeObjectURL(url);
  //     } else if (type === "mean_expression") {
  //       data = Object.entries(currentResults.gene_stats);
  //       headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])];
  //       filename = `mean_expression_${filename}.csv`;
  //       const rows = data.map(([gene, stats]) => {
  //         const metrics = selectedSites
  //           .map((site) => {
  //             const lowerSite = site.toLowerCase();
  //             const metric = stats[lowerSite] || {};
  //             return [metric.mean_normal?.toFixed(2) || "0.00", metric.mean_tumor?.toFixed(2) || "0.00"];
  //           })
  //           .flat();
  //         return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
  //       });
  //       const content = [headers.join(","), ...rows].join("\n");
  //       const blob = new Blob([content], { type: "text/csv" });
  //       const url = URL.createObjectURL(blob);
  //       const a = document.createElement("a");
  //       a.href = url;
  //       a.download = filename;
  //       a.click();
  //       URL.revokeObjectURL(url);
  //     } else if (type === "noise_metrics") {
  //       data = Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {});
  //       headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal CV`, `${site} Tumor CV`, `${site} Log2FC`])];
  //       filename = `noise_metrics_${filename}.csv`;
  //       const rows = data.map(([gene, stats]) => {
  //         const metrics = selectedSites
  //           .map((site) => {
  //             const lowerSite = site.toLowerCase();
  //             const metric = stats[lowerSite] || {};
  //             return [
  //               metric.cv_normal?.toFixed(2) || "0.00",
  //               metric.cv_tumor?.toFixed(2) || "0.00",
  //               metric.logfc?.toFixed(2) || "0.00",
  //             ];
  //           })
  //           .flat();
  //         return [gene, stats.ensembl_id || "N/A", ...metrics].join(",");
  //       });
  //       const content = [headers.join(","), ...rows].join("\n");
  //       const blob = new Blob([content], { type: "text/csv" });
  //       const url = URL.createObjectURL(blob);
  //       const a = document.createElement("a");
  //       a.href = url;
  //       a.download = filename;
  //       a.click();
  //       URL.revokeObjectURL(url);
  //     }
  //   },
  //   [enrichedPathways, filterState.normalizationMethod, resultsData, filterState.dataFormats, filterState.selectedSites]
  // );

//   const getZValues = useCallback(
//     (dataKey: "mean" | "cv" | "logfc") => {
//       const format = dataKey === "logfc" ? "log2" : filterState.dataFormats[dataKey];
//       const results = resultsData[format][filterState.normalizationMethod] || { gene_stats: {} };
//       const selectedSites = filterState.selectedSites;
//       const selectedGenes = filterState.selectedPathway ? filterState.selectedPathway.MatchingGenes : filterState.selectedGenes;

//       if (dataKey === "logfc") {
//         return Object.keys(results.gene_stats)
//           .filter((gene) => selectedGenes.includes(gene))
//           .map((gene) => {
//             const stats = results.gene_stats[gene];
//             return selectedSites.map((site) => {
//               const lowerSite = site.toLowerCase();
//               return stats[lowerSite]?.logfc || 0;
//             });
//           });
//       }

//       const values: number[] = [];
//       Object.keys(results.gene_stats)
//         .filter((gene) => selectedGenes.includes(gene))
//         .forEach((gene) => {
//           const stats = results.gene_stats[gene];
//           selectedSites.forEach((site) => {
//             const lowerSite = site.toLowerCase();
//             const metric = stats[lowerSite] || {};
//             if (dataKey === "mean") {
//               values.push(metric.mean_normal || 0, metric.mean_tumor || 0);
//             } else if (dataKey === "cv") {
//               values.push(metric.cv_normal || 0, metric.cv_tumor || 0);
//             }
//           });
//         });

//       const mean = values.length > 0 ? values.reduce((sum, val) => sum + val, 0) / values.length : 0;
//       const variance = values.length > 0 ? values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length : 0;
//       const stdDev = Math.sqrt(variance);

//       const zValues = Object.keys(results.gene_stats)
//         .filter((gene) => selectedGenes.includes(gene))
//         .map((gene) => {
//           const stats = results.gene_stats[gene];
//           return selectedSites.flatMap((site) => {
//             const lowerSite = site.toLowerCase();
//             const metric = stats[lowerSite] || {};
//             const normalValue = dataKey === "mean" ? metric.mean_normal || 0 : metric.cv_normal || 0;
//             const tumorValue = dataKey === "mean" ? metric.mean_tumor || 0 : metric.cv_tumor || 0;
//             const normalZ = stdDev !== 0 ? (normalValue - mean) / stdDev : 0;
//             const tumorZ = stdDev !== 0 ? (tumorValue - mean) / stdDev : 0;
//             return [normalZ, tumorZ];
//           });
//         });

//       return zValues;
//     },
//     [resultsData, filterState.dataFormats, filterState.normalizationMethod, filterState.selectedSites, filterState.selectedGenes, filterState.selectedPathway]
//   );

  // return (
  //   <div className="min-h-screen bg-white flex flex-col">
  //     <Header />
  //     <main className="flex-grow">
  //       <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
  //         <div className="grid grid-cols-[320px_1fr] gap-6">
  //           <div>
  //             {/* <Card className="mb-6">
  //               <CardContent className="p-4">
  //                 <div className="space-y-4">
  //                   <div>
  //                     <Label htmlFor="analysis-mode" className="text-sm font-medium">Analysis Mode</Label>
  //                     <select
  //                       id="analysis-mode"
  //                       value={filterState.analysisMode}
  //                       onChange={(e) => updateFilters({ analysisMode: e.target.value as "enrichment" | "neighbors" })}
  //                       className="w-full mt-1 p-2 border rounded"
  //                     >
  //                       <option value="enrichment">Enrichment</option>
  //                       <option value="neighbors">Neighbors</option>
  //                     </select>
  //                   </div>
  //                   {filterState.analysisMode === "enrichment" && (
  //                     <div>
  //                       <Label htmlFor="gene-input" className="text-sm font-medium">Paste Gene List</Label>
  //                       <Input
  //                         id="gene-input"
  //                         value={filterState.geneInput}
  //                         onChange={(e) => dispatch({ type: "SET_GENE_INPUT", payload: e.target.value })}
  //                         placeholder="e.g., TP53, CDKN1A, MDM2, BAX, GADD45A"
  //                         className="w-full mt-1"
  //                       />
  //                       <Button
  //                         onClick={handleGeneInputSubmit}
  //                         className="mt-2 bg-blue-600 hover:bg-blue-700"
  //                         disabled={!filterState.geneInput.trim()}
  //                       >
  //                         Analyze
  //                       </Button>
  //                     </div>
  //                   )}
  //                 </div>
  //               </CardContent>
  //             </Card> */}
              // <FilterPanel
              //   normalizationMethod={filterState.normalizationMethod}
              //   setNormalizationMethod={(value) => updateFilters({ normalizationMethod: value })}
              //   customFilters={customFilters}
              //   onFilterChange={(filterId, value) => {
              //     if (filterId === "sites") {
              //       updateFilters({ selectedSites: value });
              //     } else if (filterId === "genes") {
              //       updateFilters({ selectedGenes: value });
              //     }
              //   }}
              //   selectedValues={{
              //     sites: filterState.selectedSites,
              //     genes: filterState.selectedGenes,
              //   }}
              // />
//             </div>
//             <div className="flex-1">
//               {isLoading ? (
//                 <LoadingSpinner message="Please wait..." />
//               ) : error ? (
//                 <div className="text-center text-red-600">{error}</div>
//               ) : filterState.selectedSites.length === 0 || (filterState.selectedGenes.length === 0 && filterState.analysisMode === "enrichment") ? (
//                 <div className="text-center text-red-600">
//                   Please select at least one site and, for enrichment analysis, at least one gene.
//                 </div>
//               ) : currentResults.enrichment.length === 0 && currentResults.warning ? (
//                 <Card className="shadow-lg p-6 text-center text-yellow-700 bg-yellow-50">
//                   <p className="text-lg">Warning: {currentResults.warning}</p>
//                 </Card>
//               ) : (
//                 <>
//                   {currentResults.warning && (
//                     <Card className="shadow-lg p-6 mb-6 text-center text-yellow-700 bg-yellow-50">
//                       <p className="text-lg">Warning: {currentResults.warning}</p>
//                     </Card>
//                   )}
//                   <Link
//                     to="/pathway-analysis"
//                     className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
//                   >
//                     <ArrowLeft className="h-4 w-4 mr-2" />
//                     Back to Pathway Analysis
//                   </Link>
//                   <div className="mb-8">
//                     <div className="flex items-center justify-between mb-6">
//                       <h2 className="text-4xl font-bold text-blue-900">Results For Pathway Analysis</h2>
//                       <Button onClick={() => downloadData("enrichment")} variant="outline" size="sm">
//                         <Download className="h-4 w-4 mr-2" /> Download Enrichment CSV
//                       </Button>
//                     </div>
//                     <div className="overflow-x-auto mb-6">
//                       <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
//                         <tbody>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Analysis Mode</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.analysisMode}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Normalization</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.normalizationMethod.toUpperCase()}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Genes</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedGenes.join(", ") || "None"}</td>
//                           </tr>
//                           <tr className="border-b">
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Selected Pathway</th>
//                             <td className="py-3 px-4 text-blue-700">{filterState.selectedPathway?.Term || "None"}</td>
//                           </tr>
//                           <tr>
//                             <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Cancer Site(s)</th>
//                             <td className="py-3 px-4 text-blue-700">
//                               {filterState.selectedSites.join(", ")}
//                               {params.cancerTypes.length > 0 && ` (${params.cancerTypes.join(", ")})`}
//                             </td>
//                           </tr>
//                         </tbody>
//                       </table>
//                     </div>
//                   </div>
//                   <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-green-600 mb-2" />
//                         <div className="text-2xl font-bold text-green-600">{totalNormalSamples}</div>
//                         <div className="text-xs text-gray-600">Total Normal Samples</div>
//                       </CardContent>
//                     </Card>
//                     <Card className="border-0 shadow-lg">
//                       <CardContent className="flex flex-col items-center p-4 text-center">
//                         <Users className="h-6 w-6 text-red-600 mb-2" />
//                         <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
//                         <div className="text-xs text-gray-600">Total Tumor Samples</div>
//                       </CardContent>
//                     </Card>
//                   </div>
//                   <SampleCounts
//                     isOpen={filterState.isSampleCountsOpen}
//                     toggleOpen={() => dispatch({ type: "TOGGLE_SAMPLE_COUNTS" })}
//                     siteSampleCounts={siteSampleCounts}
//                     selectedSites={filterState.selectedSites}
//                     selectedGroups={filterState.selectedGroups}
//                   />
//                   {currentResults.enrichment.length === 0 ? (
//                     <Card className="shadow-lg p-6 text-center text-blue-700">
//                       <Activity className="w-10 h-10 mx-auto mb-3" />
//                       <p className="text-lg">No pathway enrichment results found for the selected parameters.</p>
//                     </Card>
//                   ) : (
//                     <>
//                       <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
//                         <div className="lg:col-span-2">
//                           <CollapsibleCard title="Enriched Pathways" className="h-full">
//                             <DataTable
//                               data={enrichedPathways}
//                               columns={[
//                                 { key: "Rank", header: "Rank", sortable: true },
//                                 { key: "Term", header: "Pathway/GO Term", sortable: true },
//                                 {
//                                   key: "FDR",
//                                   header: "FDR",
//                                   sortable: true,
//                                   render: (value: number) => formatFDR(value),
//                                 },
//                               ]}
//                               defaultSortKey="FDR"
//                               defaultSortOrder="asc"
//                               onRowClick={(row: Enrichment) =>
//                                 updateFilters({
//                                   selectedPathway: filterState.selectedPathway?.Term === row.Term ? null : row,
//                                   selectedGenes: filterState.selectedPathway?.Term === row.Term ? params.genes : row.MatchingGenes,
//                                 })
//                               }
//                               containerWidth="565px"
//                               rowClassName={(row) => {
//                                 const isSelected = filterState.selectedPathway?.Term === row.Term;
//                                 return isSelected ? "bg-blue-100 font-semibold" : "";
//                               }}
//                             />
//                           </CollapsibleCard>
//                         </div>
//                         <div>
//                           <CollapsibleCard title="Pathway Details" className="h-full">
//                             {filterState.selectedPathway ? (
//                               <div className="space-y-2 p-2">
//                                 <div>
//                                   <h3 className="font-semibold text-blue-700 text-sm">{filterState.selectedPathway.Term}</h3>
//                                   <p className="text-xs text-gray-600">Database: {filterState.selectedPathway.Database}</p>
//                                   <p className="text-xs text-gray-600">Description: {filterState.selectedPathway.Description || "N/A"}</p>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Statistics</h4>
//                                   <div className="grid grid-cols-2 gap-1 mt-1 text-xs">
//                                     <div>FDR:</div>
//                                     <div>{formatFDR(filterState.selectedPathway.FDR)}</div>
//                                     <div>Gene Count:</div>
//                                     <div>{filterState.selectedPathway.GeneCount}</div>
//                                     <div>Enrichment Score:</div>
//                                     <div>{filterState.selectedPathway.EnrichmentScore?.toFixed(2) || "N/A"}</div>
//                                   </div>
//                                 </div>
//                                 <div>
//                                   <h4 className="font-medium text-blue-700 text-xs">Matching Genes</h4>
//                                   <div className="flex flex-wrap gap-1 mt-1">
//                                     {filterState.selectedPathway.MatchingGenes.map((gene, idx) => (
//                                       <span key={idx} className="bg-blue-100 text-blue-800 px-1 py-0.5 rounded text-xs">
//                                         {gene}
//                                       </span>
//                                     ))}
//                                   </div>
//                                 </div>
//                               </div>
//                             ) : (
//                               <div className="text-center text-gray-500 h-full flex items-center justify-center">
//                                 <div>
//                                   <Info className="h-6 w-6 mx-auto mb-1" />
//                                   <p className="text-xs">Select a pathway to view details</p>
//                                 </div>
//                               </div>
//                             )}
//                           </CollapsibleCard>
//                         </div>
//                       </div>
//                       {filterState.analysisMode === "enrichment" && filterState.selectedPathway && (
//                         <>
//                           <CollapsibleCard
//                             title="Mean Expression"
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("mean_expression")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="mean-table-log2-switch"
//                                 checked={filterState.dataFormats.mean === "log2"}
//                                 onCheckedChange={(checked) =>
//                                   updateFilters({ dataFormats: { ...filterState.dataFormats, mean: checked ? "log2" : "raw" } })
//                                 }
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="mean-table-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                             {Object.keys(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                               .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene)).length > 0 ? (
//                               <DataTable
//                                 data={Object.entries(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                   .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                   .map(([gene, stats]) => ({ gene, ...stats }))}
//                                 columns={[
//                                   { key: "gene", header: "Gene", sortable: true },
//                                   ...filterState.selectedSites.flatMap((site) => {
//                                     const lowerSite = site.toLowerCase();
//                                     return [
//                                       {
//                                         key: `${site}-mean_normal`,
//                                         header: `${site} Normal`,
//                                         sortable: true,
//                                         render: (_: any, row: any) => {
//                                           const value = row[lowerSite]?.mean_normal;
//                                           return value != null ? value.toFixed(3) : "N/A";
//                                         },
//                                       },
//                                       {
//                                         key: `${site}-mean_tumor`,
//                                         header: `${site} Tumor`,
//                                         sortable: true,
//                                         render: (_: any, row: any) => {
//                                           const value = row[lowerSite]?.mean_tumor;
//                                           return value != null ? value.toFixed(3) : "N/A";
//                                         },
//                                       },
//                                     ];
//                                   }),
//                                 ]}
//                               />
//                             ) : (
//                               <p className="text-blue-700">No mean expression data available for pathway genes.</p>
//                             )}
//                           </CollapsibleCard>
                          // <CollapsibleCard
                          //   title="Gene Noise (CV)"
                          //   className="mb-4"
                          //   downloadButton={
                          //     <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
                          //       <Download className="h-4 w-4 mr-2" /> Download CSV
                          //     </Button>
                          //   }
                          // >
                          //   <div className="flex items-center space-x-2 mb-2 pl-6">
                          //     <Switch
                          //       id="cv-table-log2-switch"
                          //       checked={filterState.dataFormats.cv === "log2"}
                          //       onCheckedChange={(checked) =>
                          //         updateFilters({ dataFormats: { ...filterState.dataFormats, cv: checked ? "log2" : "raw" } })
                          //       }
                          //       className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
                          //     />
                          //     <Label htmlFor="cv-table-log2-switch" className="text-sm text-blue-900">
                          //       Log<sub>2</sub>(X + 1)
                          //     </Label>
                          //   </div>
//                             {Object.keys(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                               .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene)).length > 0 ? (
//                               <DataTable
//                                 data={Object.entries(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                   .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                   .map(([gene, stats]) => ({ gene, ...stats }))}
//                                 columns={[
//                                   { key: "gene", header: "Gene", sortable: true },
//                                   ...filterState.selectedSites.flatMap((site) => {
//                                     const lowerSite = site.toLowerCase();
//                                     return [
//                                       {
//                                         key: `${site}-cv_normal`,
//                                         header: `${site} Normal`,
//                                         sortable: true,
//                                         render: (_: any, row: any) => {
//                                           const value = row[lowerSite]?.cv_normal;
//                                           return value != null ? value.toFixed(3) : "N/A";
//                                         },
//                                       },
//                                       {
//                                         key: `${site}-cv_tumor`,
//                                         header: `${site} Tumor`,
//                                         sortable: true,
//                                         render: (_: any, row: any) => {
//                                           const value = row[lowerSite]?.cv_tumor;
//                                           return value != null ? value.toFixed(3) : "N/A";
//                                         },
//                                       },
//                                     ];
//                                   }),
//                                 ]}
//                               />
//                             ) : (
//                               <p className="text-blue-700">No noise metrics data available for pathway genes.</p>
//                             )}
//                           </CollapsibleCard>
//                           <CollapsibleCard
//                             title="Differential Noise - Log₂FC "
//                             className="mb-4"
//                             downloadButton={
//                               <Button onClick={() => downloadData("noise_metrics")} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             {Object.keys(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                               .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene)).length > 0 ? (
//                               <DataTable
//                                 data={Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                   .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                   .map(([gene, stats]) => ({ gene, ...stats }))}
//                                 columns={[
//                                   { key: "gene", header: "Gene", sortable: true },
//                                   ...filterState.selectedSites.flatMap((site) => {
//                                     const lowerSite = site.toLowerCase();
//                                     return [
//                                       {
//                                         key: `${site}-logfc`,
//                                         header: `${site}`,
//                                         sortable: true,
//                                         render: (_: any, row: any) => {
//                                           const value = row[lowerSite]?.logfc;
//                                           return value != null ? value.toFixed(3) : "N/A";
//                                         },
//                                       },
//                                     ];
//                                   }),
//                                 ]}
//                               />
                              
//                             ) : (
//                               <p className="text-blue-700">No differential noise data available for pathway genes.</p>
//                             )}
//                           </CollapsibleCard>
//                           <CollapsibleCard title="Mean Expression" className="mb-4">
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="mean-log2-switch"
//                                 checked={filterState.dataFormats.mean === "log2"}
//                                 onCheckedChange={(checked) =>
//                                   updateFilters({ dataFormats: { ...filterState.dataFormats, mean: checked ? "log2" : "raw" } })
//                                 }
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="mean-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                             <PlotlyHeatmap
//                               title="Mean Expression"
//                               data={Object.entries(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.mean][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("mean")}
//                               zLabel={`Mean Expression (${filterState.dataFormats.mean.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="expression-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                               // colorscale="Pastel1"
//                             />
//                           </CollapsibleCard>
//                           <CollapsibleCard title="Gene Noise (CV)" className="mb-4">
//                             <div className="flex items-center space-x-2 mb-2 pl-6">
//                               <Switch
//                                 id="cv-log2-switch"
//                                 checked={filterState.dataFormats.cv === "log2"}
//                                 onCheckedChange={(checked) =>
//                                   updateFilters({ dataFormats: { ...filterState.dataFormats, cv: checked ? "log2" : "raw" } })
//                                 }
//                                 className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
//                               />
//                               <Label htmlFor="cv-log2-switch" className="text-sm text-blue-900">
//                                 Log<sub>2</sub>(X + 1)
//                               </Label>
//                             </div>
//                             <PlotlyHeatmap
//                               title="Coefficient of Variation (Z-scores)"
//                               data={Object.entries(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])}
//                               yValues={Object.keys(resultsData[filterState.dataFormats.cv][filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("cv")}
//                               zLabel={`Noise (${filterState.dataFormats.cv.toUpperCase()} ${filterState.normalizationMethod.toUpperCase()})`}
//                               chartKey="cv-heatmap"
//                               xLabel="Sample Types"
//                               yLabel="Genes"
//                               // colorscale="Mint"
//                             />
//                           </CollapsibleCard>
//                           <CollapsibleCard title="Differential Noise">
//                             <PlotlyHeatmap
//                               title={`Log₂(CV<sub>tumor</sub> / CV<sub>normal</sub>) for Pathway Genes`}
//                               data={Object.entries(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter(([gene]) => filterState.selectedPathway?.MatchingGenes.includes(gene))
//                                 .map(([, stats]) => stats)}
//                               xValues={filterState.selectedSites}
//                               yValues={Object.keys(resultsData.log2[filterState.normalizationMethod]?.gene_stats || {})
//                                 .filter((gene) => filterState.selectedPathway?.MatchingGenes.includes(gene))}
//                               zValues={getZValues("logfc")}
//                               zLabel="Log₂FC"
//                               chartKey="logfc-heatmap"
//                               xLabel="Cancer Sites"
//                               yLabel="Genes"
//                               // colorscale="Mint"
//                             />
//                           </CollapsibleCard>
//                         </>
//                       )}
//                     </>
//                   )}
//                 </>
//               )}
//             </div>
//           </div>
//         </div>
//       </main>
//       <Footer />
//     </div>
//   );
// };

// export default React.memo(PathwayResults);