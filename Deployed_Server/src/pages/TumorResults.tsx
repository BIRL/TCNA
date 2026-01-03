import React, { useMemo, useCallback, useReducer, useEffect, useState, useRef } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Download, ChevronRight, ChevronDown, Users } from "lucide-react";
import { LoadingSpinner } from "@/components/ui/loadingSpinner";
import Header from "@/components/header";
import Footer from "@/components/footer";
import { TumorData, SampleInfo } from "@/components/types/tumor";
import FilterPanel from "@/components/FilterPanel";
import { DataTable } from "@/components/ui/data-table";
import CollapsibleCard from "@/components/ui/collapsible-card";
import PlotlyBoxPlot from "@/components/charts/PlotlyTumorBoxPlot";

// === Filter State & Reducer ===
interface FilterState {
  normalizationMethod: string;
  selectedNoiseMetrics: string[];
  metricOpenState: Record<string, boolean>;
  isStatisticalMetricsOpen: boolean;
}

type FilterAction =
  | { type: "SET_NORMALIZATION"; payload: string }
  | { type: "SET_NOISE_METRICS"; payload: string[] }
  | { type: "TOGGLE_METRIC_SECTION"; payload: string }
  | { type: "TOGGLE_STATISTICAL_METRICS" };

const initialFilterState: FilterState = {
  normalizationMethod: "tpm",
  selectedNoiseMetrics: ["DEPTH2", "DEPTH"],
  metricOpenState: { depth2: true, depth: true }, // Updated: tith → depth
  isStatisticalMetricsOpen: true,
};

const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
  switch (action.type) {
    case "SET_NORMALIZATION":
      return { ...state, normalizationMethod: action.payload };
    case "SET_NOISE_METRICS":
      return { ...state, selectedNoiseMetrics: action.payload };
    case "TOGGLE_METRIC_SECTION":
      return {
        ...state,
        metricOpenState: {
          ...state.metricOpenState,
          [action.payload]: !state.metricOpenState[action.payload],
        },
      };
    case "TOGGLE_STATISTICAL_METRICS":
      return { ...state, isStatisticalMetricsOpen: !state.isStatisticalMetricsOpen };
    default:
      return state;
  }
};

const useTumorResultsData = (
  params: { cancerSite: string; cancerTypes: string[] },
  filterState: FilterState
) => {
  // Persistent cache ref (survives re-renders)
  const cacheRef = useRef<Record<string, any>>({}); // key: normalization method

  const [state, setState] = useState<{
    rawData: { [norm: string]: TumorData[] };
    filteredData: TumorData[];
    isLoading: boolean;
    error: string | null;
    totalTumorSamples: number;
    sampleToCancerType: { [sampleId: string]: SampleInfo };
    topNoisyGenes: {
      [norm in "tpm" | "fpkm" | "fpkm_uq"]?: {
        tumor: { gene_symbol: string; cv: number }[];
        normal: { gene_symbol: string; cv: number }[];
      };
    };
  }>({
    rawData: { tpm: [], fpkm: [], fpkm_uq: [] },
    filteredData: [],
    isLoading: false,
    error: null,
    totalTumorSamples: 0,
    sampleToCancerType: {},
    topNoisyGenes: {},
  });

  const abortControllerRef = useRef<AbortController | null>(null);

  const noiseMetrics = { DEPTH2: "depth2", DEPTH: "depth" } as const;
  const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"] as const;

  const fetchAndCache = useCallback(
    async (norm: string, signal: AbortSignal) => {
      const { cancerSite, cancerTypes } = params;

      if (!cancerSite) return;

      setState((prev) => ({ ...prev, isLoading: true }));

      try {
        const queryParams = new URLSearchParams();
        queryParams.append("cancer_site", cancerSite);
        cancerTypes.forEach((type) => queryParams.append("cancer_types", type));

        const url = `/api/tumor_results?${queryParams.toString()}`;
        const response = await fetch(url, { signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const apiData = await response.json();
        if (apiData.error) throw new Error(apiData.error);

        // Cache full response per normalization
        cacheRef.current[norm] = apiData;

        // Process only for current normalization
        const sampleToCancerType: { [sampleId: string]: SampleInfo } = {};
        const dataByNorm: { [n: string]: TumorData[] } = { tpm: [], fpkm: [], fpkm_uq: [] };

        apiData.results.forEach((row: any) => {
          const sampleId = row.sample_id.toString();
          const sampleInfo: SampleInfo = {
            barcode: row.sample_barcode,
            cancerType: row.cancer_type || "Unknown",
          };
          sampleToCancerType[sampleId] = sampleInfo;

          normalizationMethods.forEach((n) => {
            const item: TumorData = {
              sample: sampleInfo.barcode,
              cancer_type: sampleInfo.cancerType,
            };

            if (filterState.selectedNoiseMetrics.includes("DEPTH2")) {
              item.depth2 = row[`${n}_depth2`];
            }
            if (filterState.selectedNoiseMetrics.includes("DEPTH")) {
              item.depth = row[`${n}_depth`];
            }

            dataByNorm[n].push(item);
          });
        });

        const topNoisy = apiData.top_noisy_genes || {};

        setState((prev) => ({
          ...prev,
          rawData: dataByNorm,
          filteredData: dataByNorm[norm],
          totalTumorSamples: apiData.sample_counts?.tumor || dataByNorm[norm].length,
          sampleToCancerType,
          topNoisyGenes: topNoisy,
          isLoading: false,
          error: null,
        }));
      } catch (err: any) {
        if (err.name === "AbortError") return;
        setState((prev) => ({
          ...prev,
          error: "Failed to load data.",
          isLoading: false,
        }));
      }
    },
    [params.cancerSite, params.cancerTypes, filterState.selectedNoiseMetrics]
  );

  // Main effect: fetch missing normalizations
  useEffect(() => {
    if (!params.cancerSite || filterState.selectedNoiseMetrics.length === 0) {
      setState(prev => ({ ...prev, error: "Select cancer site and metric", filteredData: [] }));
      return;
    }

    const missingNorms = normalizationMethods.filter(
      (norm) => !cacheRef.current[norm]
    );

    if (missingNorms.length === 0) {
      // All cached — just re-filter
      const cachedData = cacheRef.current[filterState.normalizationMethod];
      if (cachedData) {
        // Rebuild filteredData from cached raw data
        const dataByNorm = state.rawData;
        const current = dataByNorm[filterState.normalizationMethod] || [];
        const filtered = current.filter((d) =>
          filterState.selectedNoiseMetrics.some((m) => {
            const field = noiseMetrics[m as keyof typeof noiseMetrics];
            return d[field] != null && !isNaN(d[field] as number);
          })
        );

        setState(prev => ({
          ...prev,
          filteredData: filtered,
  totalTumorSamples: filtered.length || cachedData.sample_counts?.tumor || 0,
          isLoading: false,
        }));
      }
      return;
    }

    // Fetch missing normalizations one by one (or in parallel)
    missingNorms.forEach((norm) => {
      if (abortControllerRef.current) abortControllerRef.current.abort();
      const controller = new AbortController();
      abortControllerRef.current = controller;
      fetchAndCache(norm, controller.signal);
    });
  }, [
    params.cancerSite,
    params.cancerTypes,
    filterState.selectedNoiseMetrics,
    filterState.normalizationMethod, // triggers re-filter when switching
  ]);

  // Re-filter when normalization or metrics change (no refetch if cached)
  useEffect(() => {
    const currentRaw = state.rawData[filterState.normalizationMethod] || [];
    const filtered = currentRaw.filter((d) =>
      filterState.selectedNoiseMetrics.some((m) => {
        const field = noiseMetrics[m as keyof typeof noiseMetrics];
        const val = d[field];
        return val != null && !isNaN(val as number);
      })
    );

    setState(prev => ({
      ...prev,
      filteredData: filtered,
      totalTumorSamples: filtered.length,
      error: filtered.length === 0 ? "No samples match filters." : null,
    }));
  }, [filterState.normalizationMethod, filterState.selectedNoiseMetrics, state.rawData]);

  return {
    data: state.filteredData,
    isLoading: state.isLoading,
    error: state.error,
    totalTumorSamples: state.totalTumorSamples,
    sampleToCancerType: state.sampleToCancerType,
    topNoisyGenes: state.topNoisyGenes,
  };
};

// === Main Component ===
const TumourResults: React.FC = () => {
  const [searchParams] = useSearchParams();
  const rawParams = useMemo(
    () => ({
      cancerSite: searchParams.get("cancerSite") || "",
      cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
    }),
    [searchParams]
  );

  const [filterState, dispatch] = useReducer(filterReducer, initialFilterState);
  const {
    data: resultsData,
    isLoading,
    error,
    totalTumorSamples,
    sampleToCancerType,
    topNoisyGenes,
    // refetch,
  } = useTumorResultsData(rawParams, filterState);

  const noiseMetrics = { DEPTH2: "depth2", DEPTH: "depth" } as const; // Updated
  const allNoiseMetrics = Object.keys(noiseMetrics);

  const updateFilters = useCallback(
    (updates: Partial<FilterState>) => {
      if (updates.normalizationMethod) {
        dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
      }
      if (updates.selectedNoiseMetrics) {
        if (updates.selectedNoiseMetrics.length === 0) return;
        dispatch({ type: "SET_NOISE_METRICS", payload: updates.selectedNoiseMetrics });
      }
    },
    []
  );

  const handleFilterChange = useCallback(
    (filterId: string, value: string[]) => {
      if (filterId === "noiseMetrics" && value.length > 0) {
        updateFilters({ selectedNoiseMetrics: value });
      }
    },
    [updateFilters]
  );

  const downloadData = useCallback(() => {
    const keys = ["sample", "cancer_type"] as string[];
    if (filterState.selectedNoiseMetrics.includes("DEPTH2")) keys.push("depth2");
    if (filterState.selectedNoiseMetrics.includes("DEPTH")) keys.push("depth"); // Updated
    const headers = keys.join(",");
    const rows = resultsData.map((row) =>
      keys
        .map((k) => {
          const val = row[k as keyof TumorData];
          return typeof val === "number" ? val.toFixed(6) : val || "";
        })
        .join(",")
    );
    const csv = [headers, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `tumor_${rawParams.cancerSite}_${filterState.normalizationMethod}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [resultsData, rawParams.cancerSite, filterState]);

  const ithMetricsContent = (
    <div className="space-y-4 text-sm text-blue-900">
      <div>
        <h4 className="text-large font-bold">DEPTH2</h4>
        <p className="mb-2">
          DEPTH2 calculates a tumor’s ITH level based on the standard deviations of absolute z-scored expression values.
        </p>
        <a href="https://pmc.ncbi.nlm.nih.gov/articles/PMC8974098/" target="_blank" rel="noopener noreferrer" className="text-blue-600">
          Learn more
        </a>
      </div>
      <div>
        <h4 className="font-semibold">DEPTH</h4>
        <p className="mb-2">
          DEPTH calculates ITH of tumor samples with reference to matched normal samples when available.
        </p>
        <a href="https://www.nature.com/articles/s42003-020-01230-7" target="_blank" rel="noopener noreferrer" className="text-blue-600">
          Learn more
        </a>
      </div>
    </div>
  );

  const customFilters = [
    {
      title: "Metrics",
      id: "noiseMetrics",
      type: "checkbox" as const,
      options: allNoiseMetrics.map((m) => ({
        id: m,
        label: m,
        description:
          m === "DEPTH2"
            ? "Measures depth of tumor heterogeneity via gene expression variability."
            : "Quantifies ITH using transcriptomic profiles vs normal controls.",
      })),
      isMasterCheckbox: true,
      defaultOpen: false,
    },
  ];
  const NoisyGenesCard: React.FC<{
  norm: string;
  data: { tumor: typeof topNoisyGenes.tpm.tumor; normal: typeof topNoisyGenes.tpm.normal };
}> = ({ norm, data }) => {
  const [open, setOpen] = useState(true);
  return (
    <CollapsibleCard
      title={`Top 50 Noisy Genes – ${norm.toUpperCase()}`}
      defaultOpen={open}
      onToggle={setOpen}
      className="mb-4"
    >
      <div className="grid grid-cols-2 gap-4">
        {(["tumor", "normal"] as const).map((type) => (
          <div key={type}>
            <h4 className="font-semibold capitalize text-blue-800">{type}</h4>
            <ol className="list-decimal list-inside text-sm space-y-1">
              {data[type].map((g, i) => (
                <li key={i}>
                  <span className="font-mono">{g.gene_symbol}</span>{" "}
                  <span className="text-gray-600">(CV: {g.cv.toFixed(3)})</span>
                </li>
              ))}
            </ol>
          </div>
        ))}
      </div>
    </CollapsibleCard>
  );
};

  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="grid grid-cols-[320px_1fr] gap-6">
            <FilterPanel
              normalizationMethod={filterState.normalizationMethod}
              setNormalizationMethod={(v) => updateFilters({ normalizationMethod: v })}
              customFilters={customFilters}
              onFilterChange={handleFilterChange}
              selectedValues={{ noiseMetrics: filterState.selectedNoiseMetrics }}
              additionalContent={ithMetricsContent}
            />
            <div className="flex-1">
              {error && <div className="text-red-600 mb-4">{error}</div>}
              {isLoading ? (
                <LoadingSpinner message="Please wait..." />
              // ) : resultsData.length === 0 ? (
              //   <div className="text-center text-red-600">No data matches the selected filters.</div>
              ) : (
                <>
                  <Link
                    to="/tumour-analysis"
                    className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors"
                  >
                    <ArrowLeft className="h-4 w-4 mr-2" />
                    Back to Tumor Analysis
                  </Link>
                  <div className="mb-6">
                    <h2 className="text-4xl font-bold text-blue-900 mb-2">Tumor Analysis Results</h2>
                    <div className="overflow-x-auto mb-6">
                      <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
                        <tbody>
                          <tr className="border-b">
                            <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r">Normalization</th>
                            <td className="py-3 pl-2 pr-4 text-blue-700">
                              Log<sub>2</sub>({filterState.normalizationMethod.toUpperCase()} + 1)
                            </td>
                          </tr>
                          <tr>
                            <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r">Cancer Site</th>
                            <td className="py-3 pl-2 pr-4 text-blue-700">
                              {rawParams.cancerSite}
                              {rawParams.cancerTypes.length > 0 && ` (${rawParams.cancerTypes.join(", ")})`}
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                    <div className="flex justify-center items-center">
                      <Card className="border-0 shadow-lg w-full">
                        <CardContent className="flex flex-col items-center p-4 text-center">
                          <Users className="h-6 w-6 text-red-600 mb-2" />
                          <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
                          <div className="text-xs text-gray-600">Total Tumor Samples</div>
                        </CardContent>
                      </Card>
                    </div>
                  </div>

                  {(filterState.selectedNoiseMetrics.includes("DEPTH2") || filterState.selectedNoiseMetrics.includes("DEPTH")) && (
                    <div className="mb-8">
                      <div className="flex justify-between items-center mb-4">
                        <h3 className="text-2xl font-bold text-blue-900">Statistical Metrics</h3>
                        <button
                          onClick={() => dispatch({ type: "TOGGLE_STATISTICAL_METRICS" })}
                          className="text-blue-900"
                        >
                          {filterState.isStatisticalMetricsOpen ? <ChevronDown className="h-6 w-6" /> : <ChevronRight className="h-6 w-6" />}
                        </button>
                      </div>
                      {filterState.isStatisticalMetricsOpen && (
                        <>
                          {["DEPTH2", "DEPTH"].map((metric) =>
                            filterState.selectedNoiseMetrics.includes(metric) ? (
                              <CollapsibleCard
                                key={metric}
                                title={`${metric} Scores`}
                                defaultOpen={filterState.metricOpenState[metric.toLowerCase()]}
                                onToggle={(open) =>
                                  dispatch({ type: "TOGGLE_METRIC_SECTION", payload: metric.toLowerCase() })
                                }
                                className="mb-4"
                              >
                                <PlotlyBoxPlot
                                  data={resultsData}
                                  xKey={noiseMetrics[metric as keyof typeof noiseMetrics]}
                                  yLabel={`${metric} (${filterState.normalizationMethod})`}
                                  normalizationMethod={filterState.normalizationMethod}
                                  selectedGroups={[...new Set(resultsData.map((d) => d.cancer_type))]}
                                  title={`${metric} Distribution`}
                                  colorMap={{}}
                                />
                              </CollapsibleCard>
                            ) : null
                          )}
                          <CollapsibleCard
                            title="Data Table"
                            downloadButton={
                              <Button onClick={downloadData} variant="outline" size="sm" className="h-6 px-2 text-xs">
                                <Download className="h-4 w-4 mr-2" /> Download CSV
                              </Button>
                            }
                          >
                            <DataTable
                              data={resultsData}
                              columns={[
                                { key: "sample", header: "Sample", sortable: true },
                                { key: "cancer_type", header: "Cancer Project", sortable: true },
                                ...(filterState.selectedNoiseMetrics.includes("DEPTH2")
                                  ? [{ key: "depth2", header: "DEPTH2", sortable: true, render: (v) => (typeof v === "number" ? v.toFixed(4) : v) }]
                                  : []),
                                ...(filterState.selectedNoiseMetrics.includes("DEPTH")
                                  ? [{ key: "depth", header: "DEPTH", sortable: true, render: (v) => (typeof v === "number" ? v.toFixed(4) : v) }]
                                  : []),
                              ]}
                              defaultSortKey="sample"
                              scrollHeight="400px"
                              stickyHeader={true}
                            />
                          </CollapsibleCard>
                        </>
                        
                      )}
                      {/* <div className="mt-8">
                    <h3 className="text-2xl font-bold text-blue-900 mb-4">
                      Top 50 Noisy Genes (by CV)
                    </h3>
                    {(["tpm", "fpkm", "fpkm_uq"] as const).map((norm) => {
                      const genes = topNoisyGenes[norm];
                      return genes ? (
                        <NoisyGenesCard key={norm} norm={norm} data={genes} />
                      ) : null;
                    })}
                  </div> */}
                    </div>
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

export default React.memo(TumourResults);