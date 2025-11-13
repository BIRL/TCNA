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

// === Custom Hook: useTumorResultsData (No Caching) ===
const useTumorResultsData = (
  params: { cancerSite: string; cancerTypes: string[] },
  filterState: FilterState
) => {
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
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Updated: tith → depth
  const noiseMetrics = { DEPTH2: "depth2", DEPTH: "depth" } as const;
  const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"] as const;

  const fetchData = useCallback(
    async (signal: AbortSignal) => {
      const { cancerSite, cancerTypes } = params;
      const { selectedNoiseMetrics } = filterState;

      if (!cancerSite || selectedNoiseMetrics.length === 0) {
        setState((prev) => ({
          ...prev,
          error: "Cancer site and at least one metric are required.",
          filteredData: [],
          isLoading: false,
        }));
        return;
      }

      setState((prev) => ({ ...prev, isLoading: true, error: null }));

      try {
        const queryParams = new URLSearchParams();
        queryParams.append("cancer_site", cancerSite);
        cancerTypes.forEach((type) => queryParams.append("cancer_types", type));

        const response = await fetch(`/api/tumor_results?${queryParams.toString()}`, { signal });
        if (!response.ok) throw new Error(`API error: ${response.statusText}`);
        const apiData = await response.json();
        if (apiData.error) throw new Error(apiData.error);

        const topNoisy = apiData.top_noisy_genes || {};
        const sampleToCancerType: { [sampleId: string]: SampleInfo } = {};
        const dataByNorm: { [norm: string]: TumorData[] } = { tpm: [], fpkm: [], fpkm_uq: [] };

        apiData.results.forEach((row: any) => {
          const sampleId = row.sample_id.toString();
          const sampleInfo: SampleInfo = {
            barcode: row.sample_barcode,
            cancerType: row.cancer_type || "Unknown",
          };
          sampleToCancerType[sampleId] = sampleInfo;

          normalizationMethods.forEach((norm) => {
            const item: TumorData = {
              sample: sampleInfo.barcode,
              cancer_type: sampleInfo.cancerType,
            };

            if (selectedNoiseMetrics.includes("DEPTH2")) {
              item.depth2 = row[`${norm}_depth2`];
            }
            if (selectedNoiseMetrics.includes("DEPTH")) {
              item.depth = row[`${norm}_depth`]; 
            }

            dataByNorm[norm].push(item);
          });
        });

        const currentNormData = dataByNorm[filterState.normalizationMethod];
        const filtered = currentNormData.filter((d) =>
          selectedNoiseMetrics.some((m) => {
            const field = noiseMetrics[m as keyof typeof noiseMetrics];
            const val = d[field];
            return val != null && !isNaN(val);
          })
        );

        setState({
          rawData: dataByNorm,
          filteredData: filtered,
          isLoading: false,
          error: null,
          totalTumorSamples: apiData.sample_counts?.tumor || currentNormData.length,
          sampleToCancerType,
          topNoisyGenes: topNoisy,
        });
      } catch (err: any) {
        if (err.name === "AbortError") return;
        setState((prev) => ({
          ...prev,
          error: "Failed to load data. Please try again.",
          filteredData: [],
          isLoading: false,
        }));
      }
    },
    [params.cancerSite, params.cancerTypes, filterState.selectedNoiseMetrics, filterState.normalizationMethod]
  );

  const triggerFetch = useCallback(() => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    timeoutRef.current = setTimeout(() => {
      if (abortControllerRef.current) abortControllerRef.current.abort();
      const controller = new AbortController();
      abortControllerRef.current = controller;
      fetchData(controller.signal);
    }, 300);
  }, [fetchData]);

  useEffect(() => {
    triggerFetch();
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      if (abortControllerRef.current) abortControllerRef.current.abort();
    };
  }, [triggerFetch]);

  useEffect(() => {
    const current = state.rawData[filterState.normalizationMethod] || [];
    const filtered = current.filter((d) =>
      filterState.selectedNoiseMetrics.some((m) => {
        const field = noiseMetrics[m as keyof typeof noiseMetrics];
        const val = d[field];
        return val != null && !isNaN(val);
      })
    );
    setState((prev) => ({
      ...prev,
      filteredData: filtered,
      totalTumorSamples: filtered.length,
      error: filtered.length === 0 ? "No samples match selected filters." : null,
    }));
  }, [filterState.normalizationMethod, filterState.selectedNoiseMetrics, state.rawData]);

  return {
    data: state.filteredData,
    isLoading: state.isLoading,
    error: state.error,
    totalTumorSamples: state.totalTumorSamples,
    sampleToCancerType: state.sampleToCancerType,
    topNoisyGenes: state.topNoisyGenes,
    refetch: triggerFetch,
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
    refetch,
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
          DEPTH calculates a tumor’s ITH level with reference to normal controls.
        </p>
        <a href="https://www.nature.com/articles/s42003-020-01230-7" target="_blank" rel="noopener noreferrer" className="text-blue-600">
          Learn more
        </a>
      </div>
    </div>
  );

  const customFilters = [
    {
      title: "TH Metrics",
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
                <LoadingSpinner message="Loading tumor data..." />
              ) : resultsData.length === 0 ? (
                <div className="text-center text-red-600">No data matches the selected filters.</div>
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
// import React, { useMemo, useCallback, useReducer, useEffect, useState, useRef } from "react";
// import { useSearchParams, Link } from "react-router-dom";
// import { Card, CardContent } from "@/components/ui/card";
// import { Button } from "@/components/ui/button";
// import { ArrowLeft, Download, ChevronRight, ChevronDown, Users } from "lucide-react";
// import { LoadingSpinner } from "@/components/ui/loadingSpinner";
// import Header from "@/components/header";
// import Footer from "@/components/footer";
// import { TumorData, SampleInfo } from "@/components/types/tumor";
// import FilterPanel from "@/components/FilterPanel";
// import { DataTable } from "@/components/ui/data-table";
// import CollapsibleCard from "@/components/ui/collapsible-card";
// import PlotlyBoxPlot from "@/components/charts/PlotlyTumorBoxPlot";

// // === Filter State & Reducer ===
// interface FilterState {
//   normalizationMethod: string;
//   selectedNoiseMetrics: string[];
//   metricOpenState: Record<string, boolean>;
//   isStatisticalMetricsOpen: boolean;
// }

// type FilterAction =
//   | { type: "SET_NORMALIZATION"; payload: string }
//   | { type: "SET_NOISE_METRICS"; payload: string[] }
//   | { type: "TOGGLE_METRIC_SECTION"; payload: string }
//   | { type: "TOGGLE_STATISTICAL_METRICS" };

// const initialFilterState: FilterState = {
//   normalizationMethod: "tpm",
//   selectedNoiseMetrics: ["DEPTH2", "DEPTH"],
//   metricOpenState: { depth2: true, tith: true },
//   isStatisticalMetricsOpen: true,
// };

// const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
//   switch (action.type) {
//     case "SET_NORMALIZATION":
//       return { ...state, normalizationMethod: action.payload };
//     case "SET_NOISE_METRICS":
//       return { ...state, selectedNoiseMetrics: action.payload };
//     case "TOGGLE_METRIC_SECTION":
//       return {
//         ...state,
//         metricOpenState: {
//           ...state.metricOpenState,
//           [action.payload]: !state.metricOpenState[action.payload],
//         },
//       };
//     case "TOGGLE_STATISTICAL_METRICS":
//       return { ...state, isStatisticalMetricsOpen: !state.isStatisticalMetricsOpen };
//     default:
//       return state;
//   }
// };



// // === Custom Hook: useTumorResultsData (No Caching) ===
// const useTumorResultsData = (
//   params: { cancerSite: string; cancerTypes: string[] },
//   filterState: FilterState
// ) => {
//   const [state, setState] = useState<{
//     rawData: { [norm: string]: TumorData[] };
//     filteredData: TumorData[];
//     isLoading: boolean;
//     error: string | null;
//     totalTumorSamples: number;
//     sampleToCancerType: { [sampleId: string]: SampleInfo };
//     topNoisyGenes: {
//     [norm in "tpm" | "fpkm" | "fpkm_uq"]?: {
//       tumor:  { gene_symbol: string; cv: number }[];
//       normal: { gene_symbol: string; cv: number }[];
//     };
//   };
//   }>({
//     rawData: { tpm: [], fpkm: [], fpkm_uq: [] },
//     filteredData: [],
//     isLoading: false,
//     error: null,
//     totalTumorSamples: 0,
//     sampleToCancerType: {},
//     topNoisyGenes: {},
//   });

//   const abortControllerRef = useRef<AbortController | null>(null);
//   const timeoutRef = useRef<NodeJS.Timeout | null>(null);

//   const noiseMetrics = { DEPTH2: "depth2", tITH: "tith" } as const;
//   const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"] as const;

//   const fetchData = useCallback(
//     async (signal: AbortSignal) => {
//       const { cancerSite, cancerTypes } = params;
//       const { selectedNoiseMetrics } = filterState;

//       if (!cancerSite || selectedNoiseMetrics.length === 0) {
//         setState((prev) => ({
//           ...prev,
//           error: "Cancer site and at least one metric are required.",
//           filteredData: [],
//           isLoading: false,
//         }));
//         return;
//       }

//       setState((prev) => ({ ...prev, isLoading: true, error: null }));

//       try {
//         const queryParams = new URLSearchParams();
//         queryParams.append("cancer_site", cancerSite);
//         cancerTypes.forEach((type) => queryParams.append("cancer_types", type));

//         const response = await fetch(`/api/tumor_results?${queryParams.toString()}`, { signal });
//         if (!response.ok) throw new Error(`API error: ${response.statusText}`);

//         const apiData = await response.json();
//         if (apiData.error) throw new Error(apiData.error);
//         // NEW: top noisy genes (per normalization)
//         const topNoisy = apiData.top_noisy_genes || {};

//         const sampleToCancerType: { [sampleId: string]: SampleInfo } = {};
//         const dataByNorm: { [norm: string]: TumorData[] } = { tpm: [], fpkm: [], fpkm_uq: [] };

//         apiData.results.forEach((row: any) => {
//           const sampleId = row.sample_id.toString();
//           const sampleInfo: SampleInfo = {
//             barcode: row.sample_barcode,
//             cancerType: row.cancer_type || "Unknown",
//           };
//           sampleToCancerType[sampleId] = sampleInfo;

//           normalizationMethods.forEach((norm) => {
//             const item: TumorData = {
//               sample: sampleInfo.barcode,
//               cancer_type: sampleInfo.cancerType,
//             };

//             if (selectedNoiseMetrics.includes("DEPTH2")) {
//               item.depth2 = row[`${norm}_depth2`];
//             }
//             if (selectedNoiseMetrics.includes("DEPTH")) {
//               item.tith = row[`${norm}_depth`];
//             }
//             if (selectedNoiseMetrics.includes("tITH")) {
//               item.tith = row[`${norm}_tith`];
//             }

//             dataByNorm[norm].push(item);
//           });
//         });

//         const currentNormData = dataByNorm[filterState.normalizationMethod];
//         const filtered = currentNormData.filter((d) =>
//           selectedNoiseMetrics.some((m) => {
//             const field = noiseMetrics[m as keyof typeof noiseMetrics];
//             const val = d[field];
//             return val != null && !isNaN(val);
//           })
//         );

//         setState({
//           rawData: dataByNorm,
//           filteredData: filtered,
//           isLoading: false,
//           error: null,
//           totalTumorSamples: apiData.sample_counts?.tumor || currentNormData.length,
//           sampleToCancerType,
//           topNoisyGenes: topNoisy,
//         });
//       } catch (err: any) {
//         if (err.name === "AbortError") return;
//         setState((prev) => ({
//           ...prev,
//           error: "Failed to load data. Please try again.",
//           filteredData: [],
//           isLoading: false,
//         }));
//       }
//     },
//     [params.cancerSite, params.cancerTypes, filterState.selectedNoiseMetrics, filterState.normalizationMethod]
//   );

//   // Debounced fetch
//   const triggerFetch = useCallback(() => {
//     if (timeoutRef.current) clearTimeout(timeoutRef.current);
//     timeoutRef.current = setTimeout(() => {
//       if (abortControllerRef.current) abortControllerRef.current.abort();
//       const controller = new AbortController();
//       abortControllerRef.current = controller;
//       fetchData(controller.signal);
//     }, 300);
//   }, [fetchData]);

//   // Trigger fetch when params or selected metrics change
//   useEffect(() => {
//     triggerFetch();
//     return () => {
//       if (timeoutRef.current) clearTimeout(timeoutRef.current);
//       if (abortControllerRef.current) abortControllerRef.current.abort();
//     };
//   }, [triggerFetch]);

//   // Re-filter when normalization or selected metrics change
//   useEffect(() => {
//     const current = state.rawData[filterState.normalizationMethod] || [];
//     const filtered = current.filter((d) =>
//       filterState.selectedNoiseMetrics.some((m) => {
//         const field = noiseMetrics[m as keyof typeof noiseMetrics];
//         const val = d[field];
//         return val != null && !isNaN(val);
//       })
//     );

//     setState((prev) => ({
//       ...prev,
//       filteredData: filtered,
//       totalTumorSamples: filtered.length,
//       error: filtered.length === 0 ? "No samples match selected filters." : null,
//     }));
//   }, [filterState.normalizationMethod, filterState.selectedNoiseMetrics, state.rawData]);

//   return {
//     data: state.filteredData,
//     isLoading: state.isLoading,
//     error: state.error,
//     totalTumorSamples: state.totalTumorSamples,
//     sampleToCancerType: state.sampleToCancerType,
//     topNoisyGenes: state.topNoisyGenes,
//     refetch: triggerFetch,
//   };
// };


// // === Main Component ===
// const TumourResults: React.FC = () => {
//   const [searchParams] = useSearchParams();
//   const rawParams = useMemo(
//     () => ({
//       cancerSite: searchParams.get("cancerSite") || "",
//       cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
//     }),
//     [searchParams]
//   );

//   const [filterState, dispatch] = useReducer(filterReducer, initialFilterState);
//   const {
//     data: resultsData,
//     isLoading,
//     error,
//     totalTumorSamples,
//     sampleToCancerType,
//     topNoisyGenes,
//     refetch,
//   } = useTumorResultsData(rawParams, filterState);

//   const noiseMetrics = { DEPTH2: "depth2", DEPTH: "DEPTH" } as const;
//   const allNoiseMetrics = Object.keys(noiseMetrics);

//   const updateFilters = useCallback(
//     (updates: Partial<FilterState>) => {
//       if (updates.normalizationMethod) {
//         dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
//       }
//       if (updates.selectedNoiseMetrics) {
//         if (updates.selectedNoiseMetrics.length === 0) return;
//         dispatch({ type: "SET_NOISE_METRICS", payload: updates.selectedNoiseMetrics });
//       }
//     },
//     []
//   );

//   const handleFilterChange = useCallback(
//     (filterId: string, value: string[]) => {
//       if (filterId === "noiseMetrics" && value.length > 0) {
//         updateFilters({ selectedNoiseMetrics: value });
//       }
//     },
//     [updateFilters]
//   );

//   const downloadData = useCallback(() => {
//     const keys = ["sample", "cancer_type"] as string[];
//     if (filterState.selectedNoiseMetrics.includes("DEPTH2")) keys.push("depth2");
//     if (filterState.selectedNoiseMetrics.includes("DEPTH")) keys.push("depth");

//     const headers = keys.join(",");
//     const rows = resultsData.map((row) =>
//       keys
//         .map((k) => {
//           const val = row[k as keyof TumorData];
//           return typeof val === "number" ? val.toFixed(6) : val || "";
//         })
//         .join(",")
//     );

//     const csv = [headers, ...rows].join("\n");
//     const blob = new Blob([csv], { type: "text/csv" });
//     const url = URL.createObjectURL(blob);
//     const a = document.createElement("a");
//     a.href = url;
//     a.download = `tumor_${rawParams.cancerSite}_${filterState.normalizationMethod}.csv`;
//     a.click();
//     URL.revokeObjectURL(url);
//   }, [resultsData, rawParams.cancerSite, filterState]);

//   const ithMetricsContent = (
//     <div className="space-y-4 text-sm text-blue-900">
//       <div>
//         <h4 className="text-large font-bold">DEPTH2</h4>
//         <p className="mb-2">
//           DEPTH2 calculates a tumor’s ITH level based on the standard deviations of absolute z-scored expression values.
//         </p>
//         <a href="https://pmc.ncbi.nlm.nih.gov/articles/PMC8974098/" target="_blank" rel="noopener noreferrer" className="text-blue-600">
//           Learn more
//         </a>
//       </div>
//       <div>
//         <h4 className="font-semibold">DEPTH</h4>
//         <p className="mb-2">
//           DEPTH calculates a tumor’s ITH level with reference to normal controls.
//         </p>
//         <a href="https://www.nature.com/articles/s42003-020-01230-7" target="_blank" rel="noopener noreferrer" className="text-blue-600">
//           Learn more
//         </a>
//       </div>
//     </div>
//   );

//   const customFilters = [
//     {
//       title: "TH Metrics",
//       id: "noiseMetrics",
//       type: "checkbox" as const,
//       options: allNoiseMetrics.map((m) => ({
//         id: m,
//         label: m,
//         description:
//           m === "DEPTH2"
//             ? "Measures depth of tumor heterogeneity via gene expression variability."
//             : "Quantifies ITH using transcriptomic profiles vs normal controls.",
//       })),
//       isMasterCheckbox: true,
//       defaultOpen: false,
//     },
//   ];

//   return (
//     <div className="min-h-screen bg-white flex flex-col">
//       <Header />
//       <main className="flex-grow">
//         <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
//           <div className="grid grid-cols-[320px_1fr] gap-6">
//             <FilterPanel
//               normalizationMethod={filterState.normalizationMethod}
//               setNormalizationMethod={(v) => updateFilters({ normalizationMethod: v })}
//               customFilters={customFilters}
//               onFilterChange={handleFilterChange}
//               selectedValues={{ noiseMetrics: filterState.selectedNoiseMetrics }}
//               additionalContent={ithMetricsContent}
//             />

//             <div className="flex-1">
//               {error && <div className="text-red-600 mb-4">{error}</div>}

//               {isLoading ? (
//                 <LoadingSpinner message="Loading tumor data..." />
//               ) : resultsData.length === 0 ? (
//                 <div className="text-center text-red-600">No data matches the selected filters.</div>
//               ) : (
//                 <>
//                   <Link
//                     to="/tumour-analysis"
//                     className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors"
//                   >
//                     <ArrowLeft className="h-4 w-4 mr-2" />
//                     Back to Tumor Analysis
//                   </Link>

//                   <div className="mb-6">
//                     <h2 className="text-4xl font-bold text-blue-900 mb-2">Tumor Analysis Results</h2>

//                     <div className="overflow-x-auto mb-6">
//                       <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
//                         <tbody>
//                           <tr className="border-b">
//                             <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r">Normalization</th>
//                             <td className="py-3 pl-2 pr-4 text-blue-700">
//                               Log<sub>2</sub>({filterState.normalizationMethod.toUpperCase()} + 1)
//                             </td>
//                           </tr>
//                           <tr>
//                             <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r">Cancer Site</th>
//                             <td className="py-3 pl-2 pr-4 text-blue-700">
//                               {rawParams.cancerSite}
//                               {rawParams.cancerTypes.length > 0 && ` (${rawParams.cancerTypes.join(", ")})`}
//                             </td>
//                           </tr>
//                         </tbody>
//                       </table>
//                     </div>

//                     <div className="flex justify-center items-center">
//                        <Card className="border-0 shadow-lg w-full">
//                          <CardContent className="flex flex-col items-center p-4 text-center">
//                            <Users className="h-6 w-6 text-red-600 mb-2" />
//                            <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
//                           <div className="text-xs text-gray-600">Total Tumor Samples</div>
//                         </CardContent>
//                       </Card>
//                     </div>
//                   </div>

//                   {(filterState.selectedNoiseMetrics.includes("DEPTH2") || filterState.selectedNoiseMetrics.includes("tITH")) && (
//                     <div className="mb-8">
//                       <div className="flex justify-between items-center mb-4">
//                         <h3 className="text-2xl font-bold text-blue-900">Statistical Metrics</h3>
//                         <button
//                           onClick={() => dispatch({ type: "TOGGLE_STATISTICAL_METRICS" })}
//                           className="text-blue-900"
//                         >
//                           {filterState.isStatisticalMetricsOpen ? <ChevronDown className="h-6 w-6" /> : <ChevronRight className="h-6 w-6" />}
//                         </button>
//                       </div>

//                       {filterState.isStatisticalMetricsOpen && (
//                         <>
//                           {["DEPTH2", "tITH"].map((metric) =>
//                             filterState.selectedNoiseMetrics.includes(metric) ? (
//                               <CollapsibleCard
//                                 key={metric}
//                                 title={`${metric} Scores`}
//                                 defaultOpen={filterState.metricOpenState[metric.toLowerCase()]}
//                                 onToggle={(open) =>
//                                   dispatch({ type: "TOGGLE_METRIC_SECTION", payload: metric.toLowerCase() })
//                                 }
//                                 className="mb-4"
//                               >
//                                 <PlotlyBoxPlot
//                                   data={resultsData}
//                                   xKey={noiseMetrics[metric as keyof typeof noiseMetrics]}
//                                   yLabel={`${metric} (${filterState.normalizationMethod})`}
//                                   normalizationMethod={filterState.normalizationMethod}
//                                   selectedGroups={[...new Set(resultsData.map((d) => d.cancer_type))]}
//                                   title={`${metric} Distribution`}
//                                   colorMap={{}}
//                                 />
//                               </CollapsibleCard>
//                             ) : null
//                           )}

//                           <CollapsibleCard
//                             title="Data Table"
//                             downloadButton={
//                               <Button onClick={downloadData} variant="outline" size="sm" className="h-6 px-2 text-xs">
//                                 <Download className="h-4 w-4 mr-2" /> Download CSV
//                               </Button>
//                             }
//                           >
//                             <DataTable
//                               data={resultsData}
//                               columns={[
//                                 { key: "sample", header: "Sample", sortable: true },
//                                 { key: "cancer_type", header: "Cancer Project", sortable: true },
//                                 ...(filterState.selectedNoiseMetrics.includes("DEPTH2")
//                                   ? [{ key: "depth2", header: "DEPTH2", sortable: true, render: (v) => (typeof v === "number" ? v.toFixed(4) : v) }]
//                                   : []),
//                                 ...(filterState.selectedNoiseMetrics.includes("DEPTH")
//                                   ? [{ key: "tITH", header: "DEPTH", sortable: true, render: (v) => (typeof v === "number" ? v.toFixed(4) : v) }]
//                                   : []),
//                               ]}
//                               defaultSortKey="sample"
//                               scrollHeight="400px"
//                               stickyHeader={true}
//                             />
//                           </CollapsibleCard>
//                           {/* <CollapsibleCard
//                             title="Top 50 Noisy Genes"
//                             defaultOpen={false}
//                             className="mt-6"
//                           >
//                             <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
//                               {["tumor", "normal"].map((state) => {
//                                 const genes = topNoisyGenes?.[filterState.normalizationMethod]?.[state] ?? [];
//                                 return (
//                                   <div key={state} className="space-y-2">
//                                     <h4 className="font-semibold capitalize">{state} (CV %)</h4>
//                                     {genes.length === 0 ? (
//                                       <p className="text-sm text-gray-500">No data</p>
//                                     ) : (
//                                       <ol className="list-decimal pl-5 text-sm">
//                                         {genes.map((g: any, i: number) => (
//                                           <li key={i}>
//                                             <span className="font-mono">{g.gene_symbol}</span>{" "}
//                                             <span className="text-gray-600">({g.cv})</span>
//                                           </li>
//                                         ))}
//                                       </ol>
//                                     )}
//                                   </div>
//                                 );
//                               })}
//                             </div>
//                           </CollapsibleCard> */}
//                         </>
//                       )}
//                     </div>
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

// export default React.memo(TumourResults);
// import React, { useMemo, useCallback, useReducer, useEffect, useState, useRef } from "react";
// import { useSearchParams, Link } from "react-router-dom";
// import { Card, CardContent } from "@/components/ui/card";
// import { Button } from "@/components/ui/button";
// import { ArrowLeft, Download, ChevronRight, ChevronDown, Users } from "lucide-react";
// import { LoadingSpinner } from "@/components/ui/loadingSpinner";
// import Header from "@/components/header";
// import Footer from "@/components/footer";
// import { TumorData, SampleInfo } from "@/components/types/tumor";
// import { useCache } from "@/hooks/use-cache";
// import FilterPanel from "@/components/FilterPanel";
// import { DataTable } from "@/components/ui/data-table";
// import CollapsibleCard from "@/components/ui/collapsible-card";
// import PlotlyBoxPlot from "@/components/charts/PlotlyTumorBoxPlot";

// // Define interfaces for filter state
// interface FilterState {
//   normalizationMethod: string;
//   selectedNoiseMetrics: string[];
//   metricOpenState: Record<string, boolean>;
//   isStatisticalMetricsOpen: boolean;
// }

// type FilterAction =
//   | { type: "SET_NORMALIZATION"; payload: string }
//   | { type: "SET_NOISE_METRICS"; payload: string[] }
//   | { type: "TOGGLE_METRIC_SECTION"; payload: string }
//   | { type: "TOGGLE_STATISTICAL_METRICS" };

// const initialFilterState: FilterState = {
//   normalizationMethod: "tpm",
//   selectedNoiseMetrics: ["DEPTH2", "tITH"],
//   metricOpenState: { depth2: true, tith: true },
//   isStatisticalMetricsOpen: true,
// };


// const filterReducer = (state: FilterState, action: FilterAction): FilterState => {
//   switch (action.type) {
//     case "SET_NORMALIZATION":
//       return { ...state, normalizationMethod: action.payload };
//     case "SET_NOISE_METRICS":
//       return { ...state, selectedNoiseMetrics: action.payload };
//     case "TOGGLE_METRIC_SECTION":
//       return {
//         ...state,
//         metricOpenState: { ...state.metricOpenState, [action.payload]: !state.metricOpenState[action.payload] },
//       };
//     case "TOGGLE_STATISTICAL_METRICS":
//       return { ...state, isStatisticalMetricsOpen: !state.isStatisticalMetricsOpen };
//     default:
//       return state;
//   }
// };

// // Define state for tumor results data
// interface TumorResultsState {
//   rawResultsData: { [normalizationMethod: string]: TumorData[] };
//   resultsData: TumorData[];
//   isLoading: boolean;
//   error: string | null;
//   totalTumorSamples: number;
//   sampleToCancerType: { [sampleId: string]: SampleInfo };
// }

// const useTumorResultsData = (
//   params: {
//     cancerSite: string;
//     cancerTypes: string[];
//   },
//   filterState: FilterState
// ) => {
//   const [state, setState] = useState<TumorResultsState>({
//     rawResultsData: { tpm: [], fpkm: [], fpkm_uq: [] },
//     resultsData: [],
//     isLoading: false,
//     error: null,
//     totalTumorSamples: 0,
//     sampleToCancerType: {},
//   });
//   const { getCachedData, setCachedData, generateCacheKey } = useCache<TumorData[]>();
//   const filterTimeoutRef = useRef<NodeJS.Timeout | null>(null);

//   const noiseMetrics = { DEPTH2: "depth2", tITH: "tith" };
//   const validMetrics = ["DEPTH2", "tITH"];
//   const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"];

//   const fetchData = useCallback(
//     async ({
//       cancerSite,
//       cancerTypes,
//       selectedNoiseMetrics,
//     }: {
//       cancerSite: string;
//       cancerTypes: string[];
//       selectedNoiseMetrics: string[];
//     }) => {
//       console.log("fetchTumorExpressionData called with:", { cancerSite, cancerTypes, selectedNoiseMetrics });
//       if (!cancerSite || !selectedNoiseMetrics.length || state.isLoading) {
//         console.warn("Invalid parameters or already loading:", {
//           cancerSite,
//           selectedNoiseMetrics,
//           isLoading: state.isLoading,
//         });
//         setState((prev) => ({
//           ...prev,
//           error: "Missing required parameters or already loading.",
//           resultsData: [],
//           isLoading: false,
//         }));
//         return;
//       }

//       setState((prev) => ({ ...prev, isLoading: true, error: null }));
//       let sampleToCancerTypeMap: { [sampleId: number]: SampleInfo } = {};

//       try {
//         // Check cache for all normalization methods
//         const cacheKeys = normalizationMethods.reduce(
//           (acc, normMethod) => ({
//             ...acc,
//             [normMethod]: generateCacheKey({ cancerSite, cancerTypes, normalizationMethod: normMethod }),
//           }),
//           {} as { [normMethod: string]: string }
//         );

//         const cachedData: { [normMethod: string]: TumorData[] } = {};
//         let allCached = true;
//         for (const normMethod of normalizationMethods) {
//           const data = getCachedData(cacheKeys[normMethod]);
//           if (data && data.length > 0) {
//             cachedData[normMethod] = data;
//             console.log(`Cache hit for ${normMethod}:`, cacheKeys[normMethod], "Data length:", data.length);
//           } else {
//             allCached = false;
//           }
//         }

//         if (allCached) {
//           setState((prev) => ({
//             ...prev,
//             rawResultsData: { ...cachedData },
//             resultsData: cachedData[filterState.normalizationMethod].filter((sampleData) =>
//               selectedNoiseMetrics.some((metric) => {
//                 const fieldName = noiseMetrics[metric as keyof typeof noiseMetrics] || metric.toLowerCase();
//                 const value = sampleData[fieldName];
//                 return value !== null && !isNaN(value);
//               })
//             ),
//             totalTumorSamples: cachedData[filterState.normalizationMethod].length,
//             isLoading: false,
//           }));
//           return;
//         }

//         // Fetch data from API
//         const queryParams = new URLSearchParams();
//         queryParams.append("cancer_site", cancerSite);
//         if (cancerTypes.length > 0) {
//           cancerTypes.forEach((type) => queryParams.append("cancer_types", type));
//         }
//         const response = await fetch(`/api/tumor_results?${queryParams.toString()}`);
        
//         if (!response.ok) {
//           throw new Error(`API request failed: ${response.statusText}`);
//         }

//         const apiData = await response.json();
//         if (apiData.error) {
//           throw new Error(apiData.error);
//         }

//         // Process API data
//         const dataByNormalization: { [normMethod: string]: { [sampleId: string]: TumorData } } = {
//           tpm: {},
//           fpkm: {},
//           fpkm_uq: {},
//         };

//         apiData.results.forEach((row: any) => {
//           const sampleId = row.sample_id.toString();
//           const sampleInfo = {
//             barcode: row.sample_barcode,
//             cancerType: row.cancer_type || "Unknown",
//           };

//           sampleToCancerTypeMap[sampleId] = sampleInfo;

//           for (const normMethod of normalizationMethods) {
//             if (!dataByNormalization[normMethod][sampleId]) {
//               dataByNormalization[normMethod][sampleId] = {
//                 sample: sampleInfo.barcode,
//                 cancer_type: sampleInfo.cancerType,
//               };
//             }
//             if (selectedNoiseMetrics.includes("DEPTH2")) {
//               dataByNormalization[normMethod][sampleId].depth2 = row[`${normMethod}_depth2`];
//             }
//             if (selectedNoiseMetrics.includes("tITH")) {
//               dataByNormalization[normMethod][sampleId].tith = row[`${normMethod}_tith`];
//             }
//           }
//         });

//         // Process and cache data for all normalizations
//         const processedDataByNorm: { [normMethod: string]: TumorData[] } = {};
//         for (const normMethod of normalizationMethods) {
//           const processedData = Object.values(dataByNormalization[normMethod]).filter((sampleData) =>
//             validMetrics.some((metric) => {
//               const fieldName = noiseMetrics[metric as keyof typeof noiseMetrics] || metric.toLowerCase();
//               const value = sampleData[fieldName];
//               return value !== null && !isNaN(value);
//             })
//           );
//           const cacheKey = generateCacheKey({ cancerSite, cancerTypes, normalizationMethod: normMethod });
//           console.log(`Caching data for ${normMethod}:`, cacheKey, "Data length:", processedData.length);
//           setCachedData(cacheKey, processedData);
//           processedDataByNorm[normMethod] = processedData;
//         }

//         setState((prev) => ({
//           ...prev,
//           rawResultsData: { ...prev.rawResultsData, ...processedDataByNorm },
//           resultsData: processedDataByNorm[filterState.normalizationMethod].filter((sampleData) =>
//             selectedNoiseMetrics.some((metric) => {
//               const fieldName = noiseMetrics[metric as keyof typeof noiseMetrics] || metric.toLowerCase();
//               const value = sampleData[fieldName];
//               return value !== null && !isNaN(value);
//             })
//           ),
//           totalTumorSamples: apiData.sample_counts.tumor || 0,
//           sampleToCancerType: sampleToCancerTypeMap,
//           isLoading: false,
//         }));
//       } catch (error) {
//         console.error("Error during data fetch:", error);
//         setState((prev) => ({
//           ...prev,
//           error: "Failed to load data. Please check the cancer site or sample data and try again.",
//           resultsData: [],
//           isLoading: false,
//         }));
//       }
//     },
//     [getCachedData, setCachedData, generateCacheKey, state.isLoading, filterState.normalizationMethod]
//   );

//   const debouncedFetchData = useCallback(
//     (params: { cancerSite: string; cancerTypes: string[]; selectedNoiseMetrics: string[] }) => {
//       if (filterTimeoutRef.current) {
//         clearTimeout(filterTimeoutRef.current);
//       }
//       filterTimeoutRef.current = setTimeout(() => {
//         fetchData(params);
//       }, 500);
//     },
//     [fetchData]
//   );

//   // Filter rawResultsData client-side when normalizationMethod or selectedNoiseMetrics changes
//   useEffect(() => {
//     const dataForNorm = state.rawResultsData[filterState.normalizationMethod] || [];
//     if (dataForNorm.length > 0) {
//       const filteredData = dataForNorm.filter((sampleData) =>
//         filterState.selectedNoiseMetrics.some((metric) => {
//           const fieldName = noiseMetrics[metric as keyof typeof noiseMetrics] || metric.toLowerCase();
//           const value = sampleData[fieldName];
//           return value !== null && !isNaN(value);
//         })
//       );
//       setState((prev) => ({
//         ...prev,
//         resultsData: filteredData,
//         totalTumorSamples: filteredData.length,
//         error: filteredData.length === 0 ? "No data matches the selected filters." : null,
//       }));
//     }
//   }, [filterState.normalizationMethod, filterState.selectedNoiseMetrics, state.rawResultsData]);

//   // Fetch data only when cancerSite or cancerTypes changes
//   useEffect(() => {
//     const cacheKeys = normalizationMethods.reduce(
//       (acc, normMethod) => ({
//         ...acc,
//         [normMethod]: generateCacheKey({ cancerSite: params.cancerSite, cancerTypes: params.cancerTypes, normalizationMethod: normMethod }),
//       }),
//       {} as { [normMethod: string]: string }
//     );

//     const cachedData: { [normMethod: string]: TumorData[] } = {};
//     let allCached = true;
//     for (const normMethod of normalizationMethods) {
//       const data = getCachedData(cacheKeys[normMethod]);
//       if (data && data.length > 0) {
//         cachedData[normMethod] = data;
//       } else {
//         allCached = false;
//       }
//     }

//     if (allCached) {
//       console.log("Cache hit for all normalizations:", cacheKeys);
//       setState((prev) => ({
//         ...prev,
//         rawResultsData: { ...cachedData },
//         resultsData: cachedData[filterState.normalizationMethod].filter((sampleData) =>
//           filterState.selectedNoiseMetrics.some((metric) => {
//             const fieldName = noiseMetrics[metric as keyof typeof noiseMetrics] || metric.toLowerCase();
//             const value = sampleData[fieldName];
//             return value !== null && !isNaN(value);
//           })
//         ),
//         totalTumorSamples: cachedData[filterState.normalizationMethod].length,
//         isLoading: false,
//       }));
//     } else if (!state.isLoading && params.cancerSite) {
//       console.log("Cache miss for some normalizations, triggering fetch");
//       debouncedFetchData({
//         cancerSite: params.cancerSite,
//         cancerTypes: params.cancerTypes,
//         selectedNoiseMetrics: validMetrics, // Fetch all metrics to cache broadly
//       });
//     }

//     return () => {
//       if (filterTimeoutRef.current) {
//         clearTimeout(filterTimeoutRef.current);
//       }
//     };
//   }, [params.cancerSite, params.cancerTypes, getCachedData, generateCacheKey, debouncedFetchData, state.isLoading, filterState.normalizationMethod]);

//   return { ...state, fetchData: debouncedFetchData };
// };

// const TumourResults: React.FC = () => {
//   const [searchParams] = useSearchParams();
//   const rawParams = useMemo(
//     () => ({
//       cancerSite: searchParams.get("cancerSite") || "",
//       cancerTypes: searchParams.get("cancerTypes")?.split(",").filter(Boolean) || [],
//       cancerType: searchParams.get("cancerType") || "",
//     }),
//     [searchParams]
//   );

//   const [filterState, dispatch] = useReducer(filterReducer, initialFilterState);
//   const { resultsData, isLoading, error, totalTumorSamples, sampleToCancerType, fetchData } = useTumorResultsData(
//     rawParams,
//     filterState
//   );

//   const noiseMetrics = { DEPTH2: "depth2", tITH: "tith" };
//   const allNoiseMetrics = Object.keys(noiseMetrics);
//   const normalizationMethods = ["tpm", "fpkm", "fpkm_uq"];

//   const updateFilters = useCallback(
//     (updates: Partial<FilterState>) => {
//       if (updates.normalizationMethod) {
//         dispatch({ type: "SET_NORMALIZATION", payload: updates.normalizationMethod });
//       }
//       if (updates.selectedNoiseMetrics) {
//         dispatch({ type: "SET_NOISE_METRICS", payload: updates.selectedNoiseMetrics });
//       }
//     },
//     []
//   );

//   const handleFilterChange = useCallback(
//     (filterId: string, value: string[]) => {
//       console.log("handleFilterChange:", { filterId, value });
//       if (filterId === "noiseMetrics") {
//         if (value.length === 0) {
//           console.warn("At least one noise metric must remain selected.");
//           return;
//         }
//         updateFilters({ selectedNoiseMetrics: value });
//       }
//     },
//     [updateFilters]
//   );

//   const ithMetricsContent = (
//     <div className="space-y-4 text-sm text-blue-900">
//       <div>
//         <h4 className="text-large font-bold">DEPTH2</h4>
//         <p className="mb-2">
//           DEPTH2 calculates a tumor’s ITH level based on the standard deviations of absolute z-scored expression values of a set of genes in the tumor.
//         </p>
//         <a
//           href="https://pmc.ncbi.nlm.nih.gov/articles/PMC8974098/"
//           target="_blank"
//           rel="noopener noreferrer"
//           className="text-blue-600"
//         >
//           Learn more here
//         </a>
//       </div>
//       <div>
//         <h4 className="font-semibold">tITH</h4>
//         <p className="mb-2">
//           Calculated using the DEPTH algorithm, evaluating the ITH level of each tumor sample based on its gene expression profiles with reference to normal controls.
//         </p>
//         <a
//           href="https://www.nature.com/articles/s42003-020-01230-7"
//           target="_blank"
//           rel="noopener noreferrer"
//           className="text-blue-600"
//         >
//           Learn more here
//         </a>
//       </div>
//     </div>
//   );


//   const customFilters = [
//     {
//       title: "TH Metrics",
//       id: "noiseMetrics",
//       type: "checkbox" as const,
//       options: allNoiseMetrics.map((metric) => ({
//         id: metric,
//         label: metric,
//         description:
//           metric === "DEPTH2"
//             ? "DEPTH2 measures the depth of tumor heterogeneity based on gene expression variability."
//             : "tITH quantifies intra-tumor heterogeneity using transcriptomic profiles.",
//         referenceLink: "https://example.com/paper",
//       })),
//       isMasterCheckbox: true,
//       defaultOpen: false,
//     },
//   ];

//   const downloadData = useCallback(() => {
//     const keys = ["sample", "cancer_type", "depth2", "tith"].filter((key) => {
//       if (key === "depth2" && !filterState.selectedNoiseMetrics.includes("DEPTH2")) return false;
//       if (key === "tith" && !filterState.selectedNoiseMetrics.includes("tITH")) return false;
//       return true;
//     });

//     const headers = keys.join(",");
//     const rows = resultsData.map((row) =>
//       keys
//         .map((key) => {
//           const value = row[key as keyof TumorData];
//           return typeof value === "number" ? value.toFixed(6) : value || "";
//         })
//         .join(",")
//     );

//     const content = [headers, ...rows].join("\n");
//     const blob = new Blob([content], { type: "text/csv" });
//     const url = URL.createObjectURL(blob);
//     const a = document.createElement("a");
//     a.href = url;
//     a.download = `tumor_analysis_${rawParams.cancerSite}_${filterState.normalizationMethod}.csv`;
//     a.click();
//     URL.revokeObjectURL(url);
//   }, [resultsData, rawParams.cancerSite, filterState.normalizationMethod, filterState.selectedNoiseMetrics]);

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
//               onFilterChange={handleFilterChange}
//               selectedValues={{
//                 noiseMetrics: filterState.selectedNoiseMetrics,
//               }}
//               additionalContent={ithMetricsContent}
//             />
//             <div className="flex-1">
//               {error && <div className="text-red-600 mb-4">{error}</div>}
//               {isLoading ? (
//                 <LoadingSpinner message="Please wait..." />
//               ) : resultsData.length === 0 ? (
//                 <div className="text-center text-red-600">No data available for selected filters.</div>
//               ) : (
//                 <>
//                   <Link
//                     to="/tumour-analysis"
//                     className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-6 transition-colors"
//                   >
//                     <ArrowLeft className="h-4 w-4 mr-2" />
//                     Back to Tumor Analysis
//                   </Link>
//                   <div className="mb-8">
//                     <h2 className="text-4xl font-bold text-blue-900 mb-6">
//                       Results for Tumor Analysis
//                     </h2>
//                     <div className="overflow-x-auto mb-6">
//                       <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
//                         <tbody>
//                           <tr className="border-b">
//                             <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r border-gray-300">Normalization</th>
//                             <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
//                               Log<sub>2</sub>({filterState.normalizationMethod.toUpperCase()} + 1)
//                             </td>
//                           </tr>
//                           <tr>
//                             <th className="text-left py-3 pl-4 text-blue-700 font-semibold w-1/6 border-r border-gray-300">Cancer Site</th>
//                             <td className="py-3 pl-2 pr-4 text-blue-700 text-left align-top">
//                               {rawParams.cancerSite} {rawParams.cancerTypes.length > 0 && `(${rawParams.cancerTypes.join(", ")})`}
//                             </td>
//                           </tr>
//                         </tbody>
//                       </table>
//                     </div>
//                     <div className="flex justify-center items-center">
//                       <Card className="border-0 shadow-lg w-full">
//                         <CardContent className="flex flex-col items-center p-4 text-center">
//                           <Users className="h-6 w-6 text-red-600 mb-2" />
//                           <div className="text-2xl font-bold text-red-600">{totalTumorSamples}</div>
//                           <div className="text-xs text-gray-600">Total Tumor Samples</div>
//                         </CardContent>
//                       </Card>
//                     </div>
//                   </div>
//                   {(filterState.selectedNoiseMetrics.includes("DEPTH2") || filterState.selectedNoiseMetrics.includes("tITH")) && (
//                     <div className="mb-8">
//                       <div className="flex justify-between items-center mb-4">
//                         <h3 className="text-2xl font-bold text-blue-900">Statistical Metrics</h3>
//                         <button
//                           onClick={() => dispatch({ type: "TOGGLE_STATISTICAL_METRICS" })}
//                           className="text-blue-900"
//                         >
//                           {filterState.isStatisticalMetricsOpen ? (
//                             <ChevronDown className="h-6 w-6" />
//                           ) : (
//                             <ChevronRight className="h-6 w-6" />
//                           )}
//                         </button>
//                       </div>
//                       {filterState.isStatisticalMetricsOpen && (
//                         <>
//                           {["DEPTH2", "tITH"].map((metric) =>
//                             filterState.selectedNoiseMetrics.includes(metric) ? (
//                               <CollapsibleCard
//                                 key={metric}
//                                 title={`${metric} Scores`}
//                                 defaultOpen={filterState.metricOpenState[metric.toLowerCase()]}
//                                 onToggle={(open) =>
//                                   dispatch({ type: "TOGGLE_METRIC_SECTION", payload: metric.toLowerCase() })
//                                 }
//                                 className="mb-4"
//                               >
//                                 <PlotlyBoxPlot
//                                   data={resultsData}
//                                   xKey={noiseMetrics[metric as keyof typeof noiseMetrics]}
//                                   yLabel={`${metric} (${filterState.normalizationMethod})`}
//                                   normalizationMethod={filterState.normalizationMethod}
//                                   selectedGroups={[...new Set(resultsData.map((d) => d.cancer_type))]}
//                                   title={`${metric} Distribution`}
//                                   colorMap={{}}
//                                 />
//                               </CollapsibleCard>
//                             ) : null
//                           )}
//                           {resultsData.length > 0 && (
//                             <div className="mt-2">
//                               <CollapsibleCard
//                                 title="Data Table"
//                                 className="h-full"
//                                 downloadButton={
//                                   <Button
//                                     onClick={() => downloadData()}
//                                     variant="outline"
//                                     size="sm"
//                                     className="h-6 px-2 text-xs"
//                                   >
//                                     <Download className="h-4 w-4 mr-2" /> Download CSV
//                                   </Button>
//                                 }
//                               >
//                                 <DataTable
//                                   data={resultsData}
//                                   columns={[
//                                     {
//                                       key: "sample",
//                                       header: "Sample",
//                                       sortable: true,
//                                     },
//                                     {
//                                       key: "cancer_type",
//                                       header: "Cancer Project",
//                                       sortable: true,
//                                     },
//                                     ...(filterState.selectedNoiseMetrics.includes("DEPTH2")
//                                       ? [
//                                           {
//                                             key: "depth2",
//                                             header: `DEPTH2`,
//                                             sortable: true,
//                                             render: (value) => (typeof value === "number" ? value.toFixed(4) : value),
//                                           },
//                                         ]
//                                       : []),
//                                     ...(filterState.selectedNoiseMetrics.includes("tITH")
//                                       ? [
//                                           {
//                                             key: "tith",
//                                             header: `tITH`,
//                                             sortable: true,
//                                             render: (value) => (typeof value === "number" ? value.toFixed(4) : value),
//                                           },
//                                         ]
//                                       : []),
//                                   ]}
//                                   defaultSortKey="sample"
//                                   scrollHeight="400px"
//                                   stickyHeader={true}
                                  
//                                 />
//                               </CollapsibleCard>
//                             </div>
//                           )}
//                         </>
//                       )}
//                     </div>
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

// export default React.memo(TumourResults);