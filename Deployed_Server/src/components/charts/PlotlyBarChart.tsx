import React, { useRef } from "react";
import Plot from "react-plotly.js";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

interface PlotlyBarChartProps {
  data: any[];
  title?: string;
  xKey: string | string[];
  yKey?: string | string[];
  errorKey?: string | string[];
  xLabel?: string;
  yLabel?: string;
  colors?: string | string[];
  orientation?: "h" | "v";
  legendLabels?: string | string[];
  showLegend?: boolean;
  showTrendLine?: boolean;
  sortByKey?: string;
  sortOrder?: "asc" | "desc";
  hideXAxisExtras?: boolean;
  absoluteBars?: boolean;  // ← forces all bars above axis
  customData?: any[];
  hoverTemplate?: string;
}

export const PlotlyBarChart: React.FC<PlotlyBarChartProps> = ({
  data,
  title,
  xKey,
  yKey,
  errorKey,
  xLabel = "X Axis",
  yLabel = "Y Axis",
  colors = "#3b82f6",
  orientation = "h",
  legendLabels,
  showLegend = true,
  showTrendLine = false,
  sortByKey,
  sortOrder,
  hideXAxisExtras = true,
  absoluteBars = false,
  
}) => {
  const plotRef = useRef<any>(null);

  /* ---------- 1. Sorting ---------- */
  let processedData = [...data];
  if (sortByKey && sortOrder) {
    processedData.sort((a, b) => {
      const aVal = a[sortByKey] ?? 0;
      const bVal = b[sortByKey] ?? 0;
      return sortOrder === "asc" ? aVal - bVal : bVal - aVal;
    });
  }

  /* ---------- 2. Safe X value extractor ---------- */
  const getX = (row: any): any => {
    if (Array.isArray(xKey)) {
      return xKey.map((k) => row[k] ?? 0);
    }
    return row[xKey] ?? 0;
  };

  /* ---------- 3. Bar traces ---------- */
  // const barTraces: any[] = Array.isArray(yKey)
  //   ? yKey.map((key, idx) => {
  //       const yValues = processedData.map((d) => d[key] ?? 0);
  //       const absY = absoluteBars ? yValues.map(Math.abs) : yValues;

  //       return {
  //         x: processedData.flatMap(getX),
  //         y: absY,
  //         type: "bar",
  //         name: Array.isArray(legendLabels) ? legendLabels[idx] || key : legendLabels || key,
  //         marker: {
  //           color: absoluteBars
  //             ? processedData.map((d) => (d[key] < 0 ? "#ef4444c3" : "#3b83f6af"))
  //             : Array.isArray(colors)
  //             ? colors[idx % colors.length]
  //             : colors,
  //         },
  //         orientation,
  //         error_y: errorKey
  //           ? {
  //               type: "data",
  //               array: processedData.map((d) =>
  //                 Array.isArray(errorKey) ? d[errorKey[idx]] ?? 0 : d[errorKey] ?? 0
  //               ),
  //               visible: true,
  //             }
  //           : undefined,
  //       };
  //     })
  //   : (() => {
  //       const rawY = processedData.map((d) => d[yKey] ?? 0);
  //       const yValues = absoluteBars ? rawY.map(Math.abs) : rawY;

  //       return [
  //         {
  //           x: processedData.map(getX).flat(),
  //           y: yValues,
  //           type: "bar",
  //           name: legendLabels || yKey,
  //           showlegend: false,
  //           marker: {
  //             color:
  //               absoluteBars && yKey === "logfc"
  //                 ? processedData.map((d) => (d.logfc < 0 ? "#ef4444c3" : "#3b83f6af"))
  //                 : typeof colors === "string"
  //                 ? colors
  //                 : Array.isArray(colors)
  //                 ? colors
  //                 : processedData.map((d) => (d.logfc < 0 ? "#ef4444c3" : "#3b83f6af")),
  //           },
  //           orientation,
  //           error_y: errorKey
  //             ? {
  //                 type: "data",
  //                 array: processedData.map((d) =>
  //                   Array.isArray(errorKey) ? d[errorKey[0]] ?? 0 : d[errorKey] ?? 0
  //                 ),
  //                 visible: true,
  //               }
  //             : undefined,
  //         },
  //       ];
  //     })();
  /* ---------- 3. Bar traces (SPLIT BY SIGN FOR CLICKABLE LEGEND) ---------- */
const isLogfcPlot = typeof yKey === "string" && yKey === "logfc";

let barTraces: any[] = [];

if (isLogfcPlot && typeof xKey === "string") {
  /* ---- Split data ----------------------------------------------------- */
  const negData = processedData.filter((d) => (d.logfc ?? 0) < 0);
  const posData = processedData.filter((d) => (d.logfc ?? 0) >= 0);

  const getY = (d: any) => (absoluteBars ? Math.abs(d.logfc) : d.logfc);

  /* ---- Red bars (negative) ------------------------------------------- */
  if (negData.length > 0) {
    barTraces.push({
      type: "bar",
      x: negData.map((d) => d[xKey]),
      y: negData.map(getY),
      name: "Logfc < 0",
      marker: { color: "#ef4444c3" },
      orientation,
      showlegend: true,
      // legendgroup: "logfc",
    } as any);
  }

  /* ---- Blue bars (positive) ------------------------------------------ */
  if (posData.length > 0) {
    barTraces.push({
      type: "bar",
      x: posData.map((d) => d[xKey]),
      y: posData.map(getY),
      name: "Logfc > 0",
      marker: { color: "#3b83f6af" },
      orientation,
      showlegend: true,
      // legendgroup: "logfc",
    } as any);
  }
} else {
  /* ---- Fallback for every other metric (unchanged) ------------------- */
  const rawY = Array.isArray(yKey)
    ? yKey.map((k) => processedData.map((d) => d[k] ?? 0))
    : processedData.map((d) => d[yKey ?? ""] ?? 0);

  const yValues = absoluteBars ? rawY.map(Math.abs) : rawY;

  barTraces = Array.isArray(yKey)
    ? yKey.map((key, idx) => ({
        x: processedData.map(getX).flat(),
        y: yValues[idx],
        type: "bar",
        name: Array.isArray(legendLabels) ? legendLabels[idx] || key : legendLabels || key,
        marker: {
          color: Array.isArray(colors) ? colors[idx % colors.length] : colors,
        },
        orientation,
        showlegend: showLegend,
        error_y: errorKey
          ? {
              type: "data",
              array: processedData.map((d) =>
                Array.isArray(errorKey) ? d[errorKey[idx]] ?? 0 : d[errorKey] ?? 0
              ),
              visible: true,
            }
          : undefined,
      }))
    : [
        {
          x: processedData.map(getX).flat(),
          y: yValues,
          type: "bar",
          name: legendLabels || yKey,
          marker: {
            color: typeof colors === "string" ? colors : "#3b82f6",
          },
          orientation,
          showlegend: showLegend,
          error_y: errorKey
            ? {
                type: "data",
                array: processedData.map((d) =>
                  Array.isArray(errorKey) ? d[errorKey[0]] ?? 0 : d[errorKey] ?? 0
                ),
                visible: true,
              }
            : undefined,
        },
      ];
}

  /* ---------- 4. Final traces ---------- */
  const traces: any[] = [...barTraces];

/* ---------- 5. TREND LINE (connect bar tops) ---------- */
if (showTrendLine && typeof xKey === "string" && processedData.length > 1) {
  // 1. X labels: same order as sorted bars
  const xLabels: string[] = processedData.map((d) => d[xKey] as string);

  // 2. Y values: SAME as bar heights (absolute if absoluteBars=true)
  const rawY = processedData.map((d) => (d[yKey as string] as number) ?? 0);
  const yValues = absoluteBars ? rawY.map(Math.abs) : rawY;

  // 3. Push a simple line trace that connects the bar tops
  traces.push({
    type: "scatter",
    mode: "lines+markers",
    x: xLabels,
    y: yValues,
    name: "Trend",
    line: {
      color: "#000000ff",
      width: 1,
      visible: true,
      // dash: "dot",
    },
    marker: {
      size: 2,
      color: "#000000ff",
      // visible: true,
    },
    hovertemplate: `%{y:.3f}<extra></extra>`,
    showlegend: true,
  });
}

  /* ---------- 6. X-axis config ---------- */
  const xAxisConfig: Partial<Plotly.LayoutAxis> = hideXAxisExtras
    ? {
        showgrid: false,
        zeroline: false,
        showline: false,
        ticks: "",
        showticklabels: true,
      }
    : {
        showgrid: true,
        zeroline: true,
        showline: true,
        ticks: "outside",
        showticklabels: true,
      };

  /* ---------- 7. Layout ---------- */
  const layout: Partial<Plotly.Layout> = {
    title: { text: title, font: { size: 14, weight: 700 }, x: 0.5, xanchor: "center" },
    xaxis: {
      title: { text: xLabel, font: { size: 14, weight: 700 }, standoff: 20 },
      tickangle: -45,
      tickfont: { size: 10, weight: 700 },
      automargin: true,
      type: "category",
      ...xAxisConfig,
    },
    yaxis: {
      title: { text: yLabel, font: { size: 14, weight: 700 }, standoff: 20 },
      tickfont: { size: 10, weight: 700 },
      automargin: true,
      linecolor: "black",
      zeroline: true,
      showgrid: true,
      range: absoluteBars ? [0, undefined] : undefined,  // ← forces all above
    },
    showlegend: showLegend,
    legend: {
      orientation: "v",
      x: 0.02,
      xanchor: "center",
      y: 1.08,
      yanchor: "bottom",
      font: { size: 12, weight: 700 },
    },
    margin: { t: 50, b: 80, l: 60, r: 50 },
    barmode: "group",
    autosize: true,
    height: 400,
  };

  return (
    <Card className="border-0">
      <div className="flex justify-end p-2">
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            import("plotly.js-dist-min").then((Plotly) => {
              const plotEl = plotRef.current;
              if (!plotEl?.el) return console.error("Plot not ready");
              Plotly.toImage(plotEl.el, { format: "png", width: 800, height: 400 }).then(
                (url: string) => {
                  const a = document.createElement("a");
                  a.href = url;
                  a.download = `${
                    title?.replace(/\s+/g, "_").toLowerCase() || "plot"
                  }_${String(xKey).replace(/\s+/g, "_")}_${Date.now()}.png`;
                  a.click();
                }
              );
            });
          }}
          className="h-6 px-2 text-xs"
        >
          <Download className="h-3 w-3 mr-1" /> Download Plot
        </Button>
      </div>

      <CardContent className="pt-0">
        <div className="w-full max-w-[800px] mx-auto">
          <Plot
            ref={plotRef}
            data={traces}
            layout={layout}
            config={{ responsive: true }}
            style={{ width: "100%" }}
          />
        </div>
      </CardContent>
    </Card>
  );
};