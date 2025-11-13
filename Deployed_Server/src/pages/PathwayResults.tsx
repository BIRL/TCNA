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

// ====================== ENRICHMENT HOOK WITH SMART CACHING ======================
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
        cancer: sites.join(","), // same logic as before
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
        headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal`, `${site} Tumor`])];
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
        headers = ["Gene", "Ensembl ID", ...selectedSites.flatMap((site) => [`${site} Normal CV`, `${site} Tumor CV`, `${site} Log2FC`])];
        filename = `noise_metrics_${filename}.csv`;
        const rows = data.map(([gene, stats]) => {
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
                    <div className="flex items-center justify-between mb-6">
                      <h2 className="text-4xl font-bold text-blue-900">Results For Pathway Analysis</h2>
                    </div>
                    <div className="overflow-x-auto mb-6">
                      <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
                        <tbody>
                          <tr className="border-b">
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Normalization</th>
                            <td className="py-3 px-4 text-blue-700">{filterState.normalizationMethod.toUpperCase()}</td>
                          </tr>
                          {/* <tr className="border-b">
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Genes</th>
                            <td className="py-3 px-4 text-blue-700">{filterState.selectedGenes.join(", ") || "None"}</td>
                          </tr> */}
                          <tr className="border-b">
                            <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Selected Pathway</th>
                            {/* <td className="py-3 px-4 text-blue-700">
                              {(() => {
                                  const pathwayDesc =
                                    searchParams.get("pathwayDescription") ||
                                    searchParams.get("pathwayLabel") ||
                                    searchParams.get("pathwayId");
                                  return pathwayDesc ? decodeURIComponent(pathwayDesc) : "None";
                                })()} */}
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

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
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
