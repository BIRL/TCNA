import React, { useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { PlotlyBarChart } from "@/components/charts";
import CollapsibleCard from "@/components/ui/collapsible-card";
import { GeneStats } from "@/components/types/genes";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";

interface StatisticalMetricsProps {
  isOpen: boolean;
  toggleOpen: () => void;
  data: GeneStats[];
  selectedGroups: string[];
  selectedNoiseMetrics: string[];
  visiblePlots: Record<string, boolean>;
  metricOpenState: Record<string, boolean>;
  toggleMetricSection: (metric: string) => void;
  normalizationMethod: string;
  analysisType: string;
  genes: string[];
  selectedSites: string[];
}

const StatisticalMetrics: React.FC<StatisticalMetricsProps> = ({
  isOpen,
  toggleOpen,
  data,
  selectedGroups,
  selectedNoiseMetrics,
  visiblePlots,
  metricOpenState,
  toggleMetricSection,
  normalizationMethod,
  analysisType,
  genes,
  selectedSites,
}) => {
  const noiseMetrics = {
    CV: "cv",
    Mean: "mean",
    "Standard Deviation": "std",
    MAD: "mad",
    "CV²": "cv_squared",
    "Differential Noise": "logfc",
  };

  // State to manage data format for each metric independently
  const [metricDataFormats, setMetricDataFormats] = useState<Record<string, "raw" | "log2">>(
    Object.fromEntries(
      ["cv", "mean", "std", "mad", "cv_squared", "logfc"].map((metric) => [metric, "log2"])
    )
  );

  // Handler to update data format for a specific metric
  const handleMetricDataFormatChange = (metric: string, value: "raw" | "log2") => {
    console.log(`Switching ${metric} to ${value}`);
    setMetricDataFormats((prev) => ({ ...prev, [metric]: value }));
  };

  const groupedData = useMemo(() => {
    console.log("StatisticalMetrics Data Length:", data.length, "Sample:", data.slice(0, 2));
    return Object.fromEntries(
      ["cv", "mean", "std", "mad", "cv_squared", "logfc"].map((metric) => [
        metric,
        data
          .filter(
            (gene) =>
              selectedSites.includes(gene.site) &&
              genes.includes(gene.gene_symbol) &&
              gene.normalizationMethod === normalizationMethod &&
              gene.dataFormat === metricDataFormats[metric]
          )
          .reduce((acc, gene) => {
            if (!acc[gene.site]) acc[gene.site] = [];
            acc[gene.site].push(gene);
            return acc;
          }, {} as Record<string, GeneStats[]>),
      ])
    );
  }, [data, selectedSites, genes, normalizationMethod, metricDataFormats]);

  const logfcColors = useMemo(
    () =>
      Object.fromEntries(
        ["cv", "mean", "std", "mad", "cv_squared", "logfc"].map((metric) => [
          metric,
          data
            .filter((gene) => gene.dataFormat === metricDataFormats[metric])
            .map((gene) => (gene.logfc < 0 ? "#ef4444c3" : "#3b83f6af")),
        ])
      ),
    [data, metricDataFormats]
  );

  return (
    <div className="mb-8">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-2xl font-bold text-blue-900">Statistical Metrics</h3>
        <button onClick={toggleOpen} className="text-blue-900">
          {isOpen ? <ChevronDown className="h-6 w-6" /> : <ChevronRight className="h-6 w-6" />}
        </button>
      </div>
      {isOpen && (
        <>
          {["cv", "mean", "std", "mad", "cv_squared", "logfc"].map((metric) => {
            const displayMetric = Object.keys(noiseMetrics).find((key) => noiseMetrics[key] === metric) || "log2 Fold Change";
            const currentDataFormat = metricDataFormats[metric];
            const metricData = data.filter(
              (d) =>
                selectedSites.includes(d.site) &&
                genes.includes(d.gene_symbol) &&
                d.normalizationMethod === normalizationMethod &&
                d.dataFormat === currentDataFormat
            );
            console.log(`Metric ${metric} Data Length:`, metricData.length, "Format:", currentDataFormat, "Sample:", metricData.slice(0, 2));
            return (
              selectedNoiseMetrics.includes(displayMetric) && visiblePlots[metric] && (
                <div key={`global-${metric}`} className="mb-4">
                  <CollapsibleCard
                    title={`${displayMetric}`}
                    defaultOpen={metricOpenState[metric]}
                    onToggle={() => toggleMetricSection(metric)}
                  >
                    {metric !== "logfc" && (
                      <div className="flex items-center space-x-2 mb-2 pl-6">
                        <Switch
                          id={`${metric}-log2-switch`}
                          checked={currentDataFormat === "log2"}
                          onCheckedChange={(checked) => handleMetricDataFormatChange(metric, checked ? "log2" : "raw")}
                          className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
                        />
                        <Label htmlFor={`${metric}-log2-switch`} className="text-sm text-blue-900">
                          Log<sub>2</sub>(X + 1)
                        </Label>
                      </div>
                    )}
                    {metricData.length === 0 ? (
                      <div className="text-center text-red-600">
                        No {currentDataFormat === "raw" ? "raw" : "log2"} data available for {displayMetric}.
                      </div>
                    ) : (
                      <div className="w-full max-w-[800px] mx-auto">
                        {metric === "logfc" ? (
                          selectedGroups.includes("tumor") && selectedGroups.includes("normal") ? (
                            metricData.some((d) => d.normalSamples > 1) ? (
                              <PlotlyBarChart
                                key={`${metric}-${currentDataFormat}`}
                                data={metricData}
                                xKey={analysisType === "cancer-specific" ? "gene_symbol" : "site"}
                                yKey="logfc"
                                title={`Log₂(CV<sub>tumor</sub> / CV<sub>normal</sub>)`}
                                xLabel={analysisType === "cancer-specific" ? "Genes" : "Cancer Sites"}
                                yLabel={displayMetric}
                                // colors={logfcColors[metric]}
                                colors={metricData.map((d) => (d.logfc < 0 ? "#ef4444c3" : "#3b83f6af"))}
                                orientation="v"
                                showLegend={false}
                              />
                            ) : (
                              <div className="text-center text-red-600">
                                {metricData[0]?.logfcMessage || "Not enough normal samples for Differential Noise analysis."}
                              </div>
                            )
                          ) : (
                            <div className="text-center text-red-600">
                              Both tumor and normal groups must be selected for Differential Noise analysis.
                            </div>
                          )
                        ) : metric === "mad" || metric === "std" ? (
                          <PlotlyBarChart
                            key={`${metric}-${currentDataFormat}`}
                            data={metricData}
                            xKey={analysisType === "pan-cancer" ? "site" : "gene_symbol"}
                            yKey={[
                              ...(selectedGroups.includes("normal") ? ["mean_normal"] : []),
                              ...(selectedGroups.includes("tumor") ? ["mean_tumor"] : []),
                            ]}
                            errorKey={[
                              ...(selectedGroups.includes("normal") ? [`${metric}_normal`] : []),
                              ...(selectedGroups.includes("tumor") ? [`${metric}_tumor`] : []),
                            ]}
                            title={`Mean ± ${displayMetric}`}
                            xLabel={analysisType === "pan-cancer" ? "Cancer Sites" : "Genes"}
                            yLabel={`Mean ± ${displayMetric} (${normalizationMethod})`}
                            colors={selectedGroups.map((group) =>
                              group === "tumor" ? "#ef4444c3" : "#10b981bd"
                            )}
                            orientation="v"
                            legendLabels={["Normal", "Tumor"]}
                          />
                        ) : (
                          <PlotlyBarChart
                            key={`${metric}-${currentDataFormat}`}
                            data={
                              analysisType === "pan-cancer"
                                ? metricData
                                : groupedData[metric][Object.keys(groupedData[metric])[0]] || []
                            }
                            xKey={analysisType === "pan-cancer" ? "site" : "gene_symbol"}
                            yKey={[
                              ...(selectedGroups.includes("normal") ? [`${metric}_normal`] : []),
                              ...(selectedGroups.includes("tumor") ? [`${metric}_tumor`] : []),
                            ]}
                            title={`${displayMetric}`}
                            xLabel={analysisType === "pan-cancer" ? "Cancer Sites" : "Genes"}
                            yLabel={`${displayMetric} (${normalizationMethod})`}
                            colors={selectedGroups.map((group) =>
                              group === "tumor" ? "#ef4444c3" : "#10b981bd"
                            )}
                            orientation="v"
                            legendLabels={["Normal", "Tumor"]}
                          />
                        )}
                      </div>
                    )}
                  </CollapsibleCard>
                </div>
              )
            );
          })}
        </>
      )}
    </div>
  );
};

export default StatisticalMetrics;