import React, { useMemo, useCallback, useReducer, useEffect } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { ArrowLeft, Download, Users } from "lucide-react";
import { LoadingSpinner } from "@/components/ui/loadingSpinner";
import Header from "@/components/header";
import Footer from "@/components/footer";
import FilterPanel from "@/components/FilterPanel";
import SampleCounts from "@/components/SampleCounts";
import StatisticalMetrics from "@/components/statisticalMetrics";
import { GeneStats } from "@/components/types/genes";
import { DataTable } from "@/components/ui/data-table";   

interface FilterState {
  selectedGroups: string[];
  selectedGenes: string[];
  selectedSites: string[];
  normalizationMethod: string;
  selectedNoiseMetrics: string[];
  visiblePlots: Record<string, boolean>;
  metricOpenState: Record<string, boolean>;
  isStatisticalMetricsOpen: boolean;
  isAnalysisPlotsOpen: boolean;
  isSampleCountsOpen: boolean;
  dataFormat: "raw" | "log2";
  
}

type FilterAction =
  | { type: "SET_GROUPS"; payload: string[] }
  | { type: "SET_GENES"; payload: string[] }
  | { type: "SET_SITES"; payload: string[] }
  | { type: "SET_NORMALIZATION"; payload: string }
  | { type: "SET_NOISE_METRICS"; payload: string[] }
  | { type: "SET_VISIBLE_PLOTS"; payload: Record<string, boolean> }
  | { type: "TOGGLE_METRIC_SECTION"; payload: string }
  | { type: "TOGGLE_STATISTICAL_METRICS" }
  | { type: "TOGGLE_ANALYSIS_PLOTS" }
  | { type: "TOGGLE_SAMPLE_COUNTS" }
  | { type: "SET_DATA_FORMAT"; payload: "raw" | "log2" };

const initialFilterState: FilterState = {
  selectedGroups: ["normal", "tumor"],
  selectedGenes: [],
  selectedSites: [],
  normalizationMethod: "tpm",
  selectedNoiseMetrics: ["CV", "Mean", "Standard Deviation", "MAD", "CV²", "Differential Noise"],
  visiblePlots: { cv: true, mean: true, std: true, mad: true, cv_squared: true, logfc: true },
  metricOpenState: { cv: true, mean: true, std: true, mad: true, cv_squared: true, logfc: true },
  isStatisticalMetricsOpen: true,
  isAnalysisPlotsOpen: true,
  isSampleCountsOpen: true,
  dataFormat: "log2",
};

const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
  switch (action.type) {
    case "SET_GROUPS":
      return { ...state, selectedGroups: action.payload };
    case "SET_GENES":
      return { ...state, selectedGenes: action.payload };
    case "SET_SITES":
      return { ...state, selectedSites: action.payload };
    case "SET_NORMALIZATION":
      return { ...state, normalizationMethod: action.payload };
    case "SET_NOISE_METRICS":
      return { ...state, selectedNoiseMetrics: action.payload };
    case "SET_VISIBLE_PLOTS":
      return { ...state, visiblePlots: action.payload };
    case "TOGGLE_METRIC_SECTION":
      return { ...state, metricOpenState: { ...state.metricOpenState, [action.payload]: !state.metricOpenState[action.payload] } };
    case "TOGGLE_STATISTICAL_METRICS":
      return { ...state, isStatisticalMetricsOpen: !state.isStatisticalMetricsOpen };
    case "TOGGLE_ANALYSIS_PLOTS":
      return { ...state, isAnalysisPlotsOpen: !state.isAnalysisPlotsOpen };
    case "TOGGLE_SAMPLE_COUNTS":
      return { ...state, isSampleCountsOpen: !state.isSampleCountsOpen };
    case "SET_DATA_FORMAT":
      return { ...state, dataFormat: action.payload };
    default:
      return state;
  }
};

interface GeneResultsState {
  resultsData: GeneStats[];
  rawResultsData: GeneStats[];
  cachedApiData: {},
  isLoading: boolean;
  error: string | null;
  totalTumorSamples: number;
  totalNormalSamples: number;
  siteSampleCounts: { site: string; tumor: number; normal: number }[];
  availableSites: string[];
  availableGenes: string[];
  debugMessages: string[];
}

const useGeneResultsData = (
  params: {
    cancerSites: string[];
    cancerTypes: string[];
    genes: string[];
    analysisType: string;
  },
  filterState: FilterState
) => {
  /* ------------------------------------------------------------------
   * 1. STATE
   * ------------------------------------------------------------------ */
  const [state, setState] = React.useState<GeneResultsState>({
    resultsData: [],
    rawResultsData: [],
    cachedApiData: null,
    isLoading: false,
    error: null,
    totalTumorSamples: 0,
    totalNormalSamples: 0,
    siteSampleCounts: [],
    availableSites: [],
    availableGenes: [],
    debugMessages: [],
  });

  /* ------------------------------------------------------------------
   * 2. PER-SITE CACHE – **must be a hook** and declared **first**
   * ------------------------------------------------------------------ */
  const siteCache = React.useRef<Record<string, any>>({});

  /* ------------------------------------------------------------------
   * 3. PROCESS RAW API → GeneStats[]
   * ------------------------------------------------------------------ */
  const processData = useCallback(
    (apiData: any): GeneStats[] => {
      if (!apiData || !apiData.results) {
        setState((prev) => ({
          ...prev,
          resultsData: [],
          rawResultsData: [],
          siteSampleCounts: [],
          availableSites: [],
          availableGenes: [],
          totalTumorSamples: 0,
          totalNormalSamples: 0,
          error: "No data available in the API response.",
          isLoading: false,
          debugMessages: [],
        }));
        return [];
      }

      const processedData: GeneStats[] = [];
      const siteSampleCounts: { site: string; tumor: number; normal: number }[] = [];
      const availableSites: string[] = Object.keys(apiData.results);
      const availableGenes: string[] = [];
      const debugMessages: string[] = [];

      // ---- debug messages -------------------------------------------------
      for (const norm of ["tpm", "fpkm", "fpkm_uq"] as const) {
        for (const key in apiData.debug?.[norm]) {
          if (apiData.debug[norm][key]?.error) {
            debugMessages.push(apiData.debug[norm][key].error);
          }
        }
      }

      // ---- gene-symbol → ensembl map --------------------------------------
      const geneIdMap: { [symbol: string]: string } = {};
      for (const site of Object.keys(apiData.results)) {
        for (const gene of Object.keys(apiData.results[site])) {
          for (const norm of ["tpm", "fpkm", "fpkm_uq"] as const) {
            const raw = apiData.results[site][gene][norm]?.raw;
            if (raw) {
              const symbol = raw.gene_symbol || gene;
              geneIdMap[symbol] = gene;
              if (!availableGenes.includes(symbol)) availableGenes.push(symbol);
            }
          }
        }
      }

      // ---- build GeneStats ------------------------------------------------
      for (const site of Object.keys(apiData.results)) {
        const counts = apiData.sample_counts[site] || { tumor: 0, normal: 0 };
        siteSampleCounts.push({ site, tumor: counts.tumor, normal: counts.normal });

        const siteData = apiData.results[site];
        for (const gene of Object.keys(siteData)) {
          const geneData = siteData[gene];
          const ensemblId = geneIdMap[gene] || gene;

          for (const norm of ["tpm", "fpkm", "fpkm_uq"] as const) {
            for (const format of ["raw", "log2"] as const) {
              const data = geneData[norm]?.[format] || {};

              const tumorValues: number[] = [];
              const normalValues: number[] = [];

              const noiseMetrics = {
                CV: "cv",
                Mean: "mean",
                "Standard Deviation": "std",
                MAD: "mad",
                "CV²": "cv_squared",
                "Differential Noise": "logfc",
              } as const;

              const metrics: any = {};
              Object.entries(noiseMetrics).forEach(([label, key]) => {
                const tumorKey = `${key}_tumor`;
                const normalKey = `${key}_normal`;
                const tumorVal = data[key] ?? undefined;
                const normalVal = data[`${key}_normal`] ?? undefined;

                metrics[`${key}_tumor_${norm}`] = tumorVal;
                metrics[`${key}_normal_${norm}`] = normalVal;

                if (key === "logfc") metrics[`logfc_${norm}`] = data.logfc ?? undefined;

                if (tumorVal !== undefined) tumorValues.push(tumorVal);
                if (normalVal !== undefined && key !== "logfc") normalValues.push(normalVal);
              });

              const geneStat: GeneStats = {
                gene: `${data.gene_symbol || gene} (${gene})`,
                ensembl_id: ensemblId,
                gene_symbol: data.gene_symbol || gene,
                tumorValues: tumorValues.length ? tumorValues : undefined,
                normalValues: normalValues.length ? normalValues : undefined,
                cv_tumor: metrics[`cv_tumor_${norm}`] ?? undefined,
                mean_tumor: metrics[`mean_tumor_${norm}`] ?? undefined,
                std_tumor: metrics[`std_tumor_${norm}`] ?? undefined,
                mad_tumor: metrics[`mad_tumor_${norm}`] ?? undefined,
                cv_squared_tumor: metrics[`cv_squared_tumor_${norm}`] ?? undefined,
                cv_normal: metrics[`cv_normal_${norm}`] ?? undefined,
                mean_normal: metrics[`mean_normal_${norm}`] ?? undefined,
                std_normal: metrics[`std_normal_${norm}`] ?? undefined,
                mad_normal: metrics[`mad_normal_${norm}`] ?? undefined,
                cv_squared_normal: metrics[`cv_squared_normal_${norm}`] ?? undefined,
                tumorSamples: counts.tumor,
                normalSamples: counts.normal,
                logfc: metrics[`logfc_${norm}`] ?? undefined,
                logfcMessage: counts.normal < 5 ? "Warning: Low number of normal samples may affect differential results." : undefined,
                site,
                normalizationMethod: norm,
                dataFormat: format,
                ...metrics,
              };
              processedData.push(geneStat);
            }
          }
        }
      }

      // ---- apply current UI filters ---------------------------------------
      const filteredData = processedData.filter(
        (d) =>
          filterState.selectedSites.includes(d.site) &&
          filterState.selectedGenes.includes(d.gene_symbol) &&
          d.normalizationMethod === filterState.normalizationMethod &&
          d.dataFormat === filterState.dataFormat
      );

      // ---- update hook state ---------------------------------------------
      setState((prev) => ({
        ...prev,
        resultsData: filteredData,
        rawResultsData: processedData,
        siteSampleCounts,
        availableSites,
        availableGenes,
        totalTumorSamples: siteSampleCounts.reduce((s, c) => s + c.tumor, 0),
        totalNormalSamples: siteSampleCounts.reduce((s, c) => s + c.normal, 0),
        error:
          debugMessages.length > 0 && filteredData.length === 0
            ? `No data for selected filters (sites: ${filterState.selectedSites}, genes: ${filterState.selectedGenes}, norm: ${filterState.normalizationMethod}, fmt: ${filterState.dataFormat}).`
            : null,
        isLoading: false,
        debugMessages,
      }));

      return filteredData;
    },
    [filterState]
  );

  /* ------------------------------------------------------------------
   * MERGE ALL CACHED SITES → single API-shaped object
   * ------------------------------------------------------------------ */
  const mergeAllCached = useCallback(() => {
    const merged: any = { results: {}, sample_counts: {}, debug: {} };
    for (const site of Object.keys(siteCache.current)) {
      const cached = siteCache.current[site];
      merged.results[site] = cached.results;
      merged.sample_counts[site] = cached.sample_counts;
      if (!merged.debug || Object.keys(merged.debug).length === 0) {
        merged.debug = cached.debug ?? {};
      }
    }
    return merged;
  }, []);

  /* ------------------------------------------------------------------
   * SMART FETCH – only requests missing sites
   * ------------------------------------------------------------------ */
  const fetchData = useCallback(
    async (sitesToFetch: string[]) => {
      if (!sitesToFetch.length || !params.genes.length) {
        setState((prev) => ({
          ...prev,
          error: "Please select at least one cancer site and one gene.",
          isLoading: false,
          debugMessages: [],
        }));
        return;
      }

      const missingSites = sitesToFetch.filter((s) => !siteCache.current[s]);

      if (missingSites.length === 0) {
        processData(mergeAllCached());
        return;
      }

      setState((prev) => ({ ...prev, isLoading: true, error: null, debugMessages: [] }));

      try {
        const qp = new URLSearchParams();
        sitesToFetch.forEach((s) => qp.append("cancer_site", s));
        if (params.cancerTypes.length) params.cancerTypes.forEach((t) => qp.append("cancer_type", t));
        params.genes.forEach((g) => qp.append("gene_ids", g));

        console.log("FETCHING:", `/api/gene_noise?${qp}`);   // <-- DEBUG LOG

        const resp = await fetch(`/api/gene_noise?${qp}`, {
          method: "GET",
          headers: { "Content-Type": "application/json" },
        });

        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`HTTP ${resp.status}: ${txt}`);
        }

        const fresh = await resp.json();
        if (fresh.error) throw new Error(fresh.error);

        // store ONLY the new sites
        for (const site of Object.keys(fresh.results ?? {})) {
          if (missingSites.includes(site)) {
            siteCache.current[site] = {
              results: fresh.results[site],
              sample_counts: fresh.sample_counts?.[site],
              debug: fresh.debug,
            };
          }
        }

        const merged = mergeAllCached();
        setState((prev) => ({ ...prev, cachedApiData: merged }));
        processData(merged);
      } catch (e: any) {
        console.error("FETCH ERROR:", e);
        setState((prev) => ({
          ...prev,
          error: e.message || "Network error",
          isLoading: false,
          debugMessages: [],
        }));
      }
    },
    [params, processData, mergeAllCached]
  );

  /* ------------------------------------------------------------------
   * 1. INITIAL LOAD (URL params)
   * ------------------------------------------------------------------ */
  useEffect(() => {
    if (params.cancerSites.length && params.genes.length) {
      fetchData(params.cancerSites);
    }
  }, [params.cancerSites, params.genes, fetchData]);

  /* ------------------------------------------------------------------
   * 2. RE-FETCH WHEN USER CHANGES SITES FILTER
   * ------------------------------------------------------------------ */
  useEffect(() => {
    // This runs **after** the reducer updates `filterState.selectedSites`
    if (filterState.selectedSites.length && params.genes.length) {
      fetchData(filterState.selectedSites);
    }
  }, [filterState.selectedSites, params.genes, fetchData]);

  return { ...state, fetchData };
};

const GeneResults: React.FC = () => {
  const [searchParams] = useSearchParams();
  const params = useMemo(() => ({
    cancerSites: searchParams.get("sites")?.split(",").filter(Boolean) || [],
    cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
    genes: searchParams.get("genes")?.split(",").filter(Boolean) || [],
    analysisType: searchParams.get("analysisType") || "cancer-specific",
  }), [searchParams]);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);
  const [allAvailableSites, setAllAvailableSites] = React.useState<string[]>([]);

  useEffect(() => {
    const fetchAllSites = async () => {
      try {
        const response = await fetch("/api/sites");
        if (!response.ok) throw new Error("Failed to fetch sites");
        const data = await response.json();
        const sites = data.sites.map((s: { name: string }) => s.name).sort();
        setAllAvailableSites(sites);
      } catch (err) {
        console.error("Error fetching sites:", err);
      }
    };
    fetchAllSites();
  }, []);

  const [filterState, dispatch] = useReducer(filterReducer, {
    ...initialFilterState,
    selectedGenes: params.genes,
    selectedSites: params.cancerSites,
  });
  

  const { resultsData, rawResultsData, isLoading, error, totalTumorSamples, totalNormalSamples, siteSampleCounts, availableSites, availableGenes, fetchData, debugMessages } = useGeneResultsData(params, filterState);

  const updateFilters = useCallback((updates: Partial<FilterState>) => {
    console.log("Updating filters:", updates);
    if (updates.selectedGroups) dispatch({ type: "SET_GROUPS", payload: updates.selectedGroups });
    if (updates.selectedGenes) dispatch({ type: "SET_GENES", payload: updates.selectedGenes });
    if (updates.selectedSites) dispatch({ type: "SET_SITES", payload: updates.selectedSites });
    if (updates.normalizationMethod) dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
    if (updates.selectedNoiseMetrics) dispatch({ type: "SET_NOISE_METRICS", payload: updates.selectedNoiseMetrics });
    if (updates.visiblePlots) dispatch({ type: "SET_VISIBLE_PLOTS", payload: updates.visiblePlots });
    if (updates.dataFormat) dispatch({ type: "SET_DATA_FORMAT", payload: updates.dataFormat });
  }, []);

const handleFilterChange = useCallback(
  (filterId: string, value: any) => {
    if (filterId === "sites") {
      // 1. Update reducer
      updateFilters({ selectedSites: value });

      // 2. **fetchData is now called automatically** by the useEffect above
      //    → no manual call needed here
    } else if (filterId === "genes") {
      updateFilters({ selectedGenes: value });
    } else if (filterId === "noiseMetrics") {
      updateFilters({ selectedNoiseMetrics: value });
    } else if (filterId === "analysisPlots") {
      updateFilters({
        visiblePlots: {
          ...filterState.visiblePlots,
          ...Object.fromEntries(
            value.map((id: string) => [id, true]).concat(
              Object.keys(filterState.visiblePlots)
                .filter((k) => !value.includes(k))
                .map((k) => [k, false])
            )
          ),
        },
      });
    }
  },
  [filterState.visiblePlots, updateFilters]   // <-- fetchData removed – handled by useEffect
);

  // const downloadData = useCallback((format: "csv") => {
  //   const data = resultsData.filter((d) => d.normalizationMethod === filterState.normalizationMethod);
  //   let content = "";
  //   let filename = `gene_analysis_${filterState.normalizationMethod}.csv`;

  //   if (format === "csv") {
  //     const keys = [
  //       "site", "gene_symbol", "ensembl_id", "dataFormat",
  //       "cv_tumor", "mean_tumor", "std_tumor", "mad_tumor", "cv_squared_tumor",
  //       "cv_normal", "mean_normal", "std_normal", "mad_normal", "cv_squared_normal",
  //       "logfc", "tumorSamples", "normalSamples"
  //     ];
  //     const headers = keys.join(",");
  //     const rows = data.map((row) =>
  //       keys.map((key) => {
  //         const value = row[key as keyof GeneStats];
  //         return typeof value === "number" ? value.toFixed(6) : value || "";
  //       }).join(",")
  //     );
  //     content = [headers, ...rows].join("\n");
  //   }

  //   const blob = new Blob([content], { type: "text/csv" });
  //   const url = URL.createObjectURL(blob);
  //   const a = document.createElement("a");
  //   a.href = url;
  //   a.download = filename;
  //   a.click();
  //   URL.revokeObjectURL(url);
  // }, [resultsData, filterState.normalizationMethod]);

  
  const downloadData = useCallback((format: "csv") => {
  if (format !== "csv") return;

  // Use rawResultsData which contains ALL processed entries (both raw & log2)
  const data = rawResultsData.filter(
    (d) => d.normalizationMethod === filterState.normalizationMethod
  );

  if (data.length === 0) {
    alert("No data available to download.");
    return;
  }

  // Define CSV columns
  const keys = [
    "site",
    "gene_symbol",
    "ensembl_id",
    "dataFormat", // This will be "raw" or "log2"
    "cv_tumor",
    "mean_tumor",
    "std_tumor",
    "mad_tumor",
    "cv_squared_tumor",
    "cv_normal",
    "mean_normal",
    "std_normal",
    "mad_normal",
    "cv_squared_normal",
    "logfc",
    "tumorSamples",
    "normalSamples",
  ] as const;

  // Build header
  const headers = keys.join(",");

  // Build rows (both raw and log2 will appear naturally since they're in rawResultsData)
  const rows = data.map((row) =>
    keys
      .map((key) => {
        const value = row[key];
        return typeof value === "number" ? value.toFixed(6) : value ?? "";
      })
      .join(",")
  );

  // Combine header + rows
  const content = [headers, ...rows].join("\n");

  // Trigger download
  const filename = `gene_analysis_${filterState.normalizationMethod}_raw_and_log2_${Date.now()}.csv`;
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}, [rawResultsData, filterState.normalizationMethod]);

  const metricFormulas = {
    CV: <>CV = σ / μ</>,
    "Standard Deviation": "σ",
    "Mean": <>μ</>,
    MAD: <>Mean Absolute Deviation</>,
    "CV²": <>CV<sup>2</sup> = (σ / μ)<sup>2</sup></>,
    "Differential Noise": <>Differential Noise = log<sub>2</sub>(CV<sub>tumor</sub> / CV<sub>normal</sub>)</>,
  };

  const customFilters = useMemo(() => {
    const filters = [];
    // show sites in pan-cancer mdoe)
    if (params.analysisType === "pan-cancer") {
      filters.push({
        title: "Sites",
        id: "sites",
        type: "checkbox" as const,
        options: allAvailableSites.map((site) => ({ id: site, label: site })),
        isMasterCheckbox: true,
        defaultOpen: false, 
      });
    }

    // Show Genes filter in cancer specifc mode
    if (params.analysisType === "cancer-specific") {
      filters.push({
        title: "Genes",
        id: "genes",
        type: "checkbox" as const,
        options: availableGenes.map((geneSymbol) => ({ id: geneSymbol, label: geneSymbol })),
        isMasterCheckbox: true,
        defaultOpen: false,
      });
    }

    // Metrics filter
    filters.push({
      title: "Metrics",
      id: "noiseMetrics",
      type: "checkbox" as const,
      options: Object.keys(metricFormulas).map((metric) => ({
        id: metric,
        label: metric,
        tooltip: metricFormulas[metric],
      })),
      isMasterCheckbox: true,
      defaultOpen: false,
    });

    return filters;
  }, [allAvailableSites, availableGenes, params.analysisType, metricFormulas]);

  const warnings = useMemo(() => {
    return [...new Set([...resultsData.map((d) => d.logfcMessage).filter(Boolean), ...debugMessages])] as string[];
  }, [resultsData, debugMessages]);

  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8"> */}
          <div className="grid grid-cols-[320px_1fr] gap-6">
            <FilterPanel
              normalizationMethod={filterState.normalizationMethod}
              setNormalizationMethod={(value) => updateFilters({ normalizationMethod: value })}
              customFilters={customFilters}
              onFilterChange={handleFilterChange}
              selectedValues={{
                sites: filterState.selectedSites,
                genes: filterState.selectedGenes,
                noiseMetrics: filterState.selectedNoiseMetrics,
              }}
            />
            <div className="flex-1">
              {isLoading ? (
                <LoadingSpinner message="Please wait..." />
              ) : error ? (
                <div className="text-center text-red-600">{error}</div>
              ) : filterState.selectedGenes.length === 0 || filterState.selectedSites.length === 0 ? (
                <div className="text-center text-red-600">Please select at least one gene or one site.</div>
              ) : resultsData.length === 0 ? (
                <div className="text-center text-red-600">No data available for selected sites, genes, normalization method, or data format.</div>
              ) : (
                <>
                  <Link
                    to="/gene-analysis"
                    className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
                  >
                    <ArrowLeft className="h-4 w-4 mr-2" />
                    Back to Gene Analysis
                  </Link>
                  <div className="mb-8">
                    <div className="flex items-center justify-between mb-6">
                      <h2 className="text-4xl font-bold text-blue-900">Results For Gene Analysis</h2>
                      <Button onClick={() => downloadData("csv")} variant="outline" size="sm">
                        <Download className="h-4 w-4 mr-2" /> Download CSV
                      </Button>
                    </div>
                    <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
                      <tbody>
                        <tr className="border-b">
                          <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r border-gray-300">
                            Analysis Type
                          </th>
                          <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
                            {params.analysisType === "pan-cancer" ? "Pan-Cancer" : "Cancer-Specific"}
                          </td>
                        </tr>
                        <tr className="border-b">
                          <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/5 border-r border-gray-300">
                            Normalization
                          </th>
                          <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
                            {filterState.normalizationMethod.toUpperCase()}
                          </td>
                        </tr>
                        <tr className="border-b">
                          <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/5 border-r border-gray-300">
                            Genes
                          </th>
                          <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
                            {filterState.selectedGenes.join(", ")}
                          </td>
                        </tr>
                        <tr>
                          <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/5 border-r border-gray-300">
                            Cancer Site(s)
                          </th>
                          <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
                            {filterState.selectedSites.join(", ")}
                            {params.cancerTypes.length > 0 && ` (${params.cancerTypes.join(", ")})`}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                    {warnings.length > 0 && (
                      <Alert className="mb-4">
                        <AlertDescription>{warnings.join("; ")}</AlertDescription>
                      </Alert>
                    )}
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
                    <StatisticalMetrics
                      isOpen={filterState.isStatisticalMetricsOpen}
                      toggleOpen={() => dispatch({ type: "TOGGLE_STATISTICAL_METRICS" })}
                      data={rawResultsData}
                      selectedGroups={filterState.selectedGroups}
                      selectedNoiseMetrics={filterState.selectedNoiseMetrics}
                      visiblePlots={filterState.visiblePlots}
                      metricOpenState={filterState.metricOpenState}
                      toggleMetricSection={(metric) => dispatch({ type: "TOGGLE_METRIC_SECTION", payload: metric })}
                      normalizationMethod={filterState.normalizationMethod}
                      analysisType={params.analysisType}
                      genes={filterState.selectedGenes}
                      selectedSites={filterState.selectedSites}
                    />
                    
                  </div>
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

export default React.memo(GeneResults);
