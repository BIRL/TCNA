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
  normalization?: string;
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
}) => {
  const plotRef = useRef<any>(null);

  const plotData = Array.isArray(yKey)
    ? yKey.map((key, index) => ({
        x: Array.isArray(xKey)
          ? xKey.map((k) => data.map((d) => d[k] || 0)).flat()
          : data.map((d) => d[xKey] || 0),
        y: data.map((d) => d[key] || 0),
        type: "bar",
        name: Array.isArray(legendLabels) ? legendLabels[index] || key : legendLabels || key,
        marker: {
          color: Array.isArray(colors) ? colors[index % colors.length] : colors,
        },
        orientation,
        error_y: errorKey
          ? {
              type: "data",
              array: data.map((d) =>
                Array.isArray(errorKey) ? d[errorKey[index]] || 0 : d[errorKey] || 0
              ),
              visible: true,
            }
          : undefined,
      }))
    : Array.isArray(xKey)
    ? xKey.map((key, index) => ({
        x: data.map((d) => d[key] || 0),
        y: data.map((d) => d[yKey] || 0),
        type: "bar",
        name: Array.isArray(legendLabels) ? legendLabels[index] || key : legendLabels || key,
        marker: {
          color: Array.isArray(colors) ? colors[index % colors.length] : colors,
        },
        orientation,
        error_y: errorKey
          ? {
              type: "data",
              array: data.map((d) =>
                Array.isArray(errorKey) ? d[errorKey[index]] || 0 : d[errorKey] || 0
              ),
              visible: true,
            }
          : undefined,
      }))
    : data.map((d, index) => ({
        x: [d[xKey] || 0],
        y: [d[yKey] || 0],
        type: "bar",
        name: Array.isArray(legendLabels) ? legendLabels[index] || d[xKey] || yKey : legendLabels || yKey,
        marker: {
          color: Array.isArray(colors) ? colors[index % colors.length] : colors,
        },
        orientation,
        error_y: errorKey
          ? {
              type: "data",
              array: [Array.isArray(errorKey) ? d[errorKey[index]] || 0 : d[errorKey] || 0],
              visible: true,
            }
          : undefined,
      }));

  return (
    <Card className="border-0">
      <div className="flex justify-end p-2">
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            import("plotly.js-dist-min").then((Plotly) => {
              const plotEl = plotRef.current;
              if (!plotEl || !plotEl.el) {
                console.error("Plot not ready yet");
                return;
              }
              Plotly.toImage(plotEl.el, {
                format: "png",
                width: 800,
                height: 400,
              }).then((url: string) => {
                const a = document.createElement("a");
                a.href = url;
                a.download = `${title?.replace(/\s+/g, "_").toLowerCase()|| "plot"}_${xKey}_${Date.now()}.png`;
                a.click();
              });
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
            data={plotData}
            layout={{
              title: { text: title, font: { size: 14, weight: "bold" }, x: 0.5, xanchor: "center" },
              xaxis: {
                title: { text: xLabel, font: { size: 14, weight: "bold" }, pad: 20, standoff: 20 },
                tickangle: -45,
                tickfont: { size: 10, weight: "bold", pad: 20 },
                automargin: true,
              },
              yaxis: {
                title: { text: yLabel, font: { size: 14, weight: "bold" }, standoff: 20 },
                tickfont: { size: 10, weight: "bold" },
                automargin: true,
                linecolor: "black",
              },
              showlegend: showLegend,
              legend: {
                orientation: "v",
                x: 0.02,
                xanchor: "center",
                y: 1.08,
                yanchor: "bottom",
                font: { size: 12, weight: "bold" },
              },
              margin: { t: 50, b: 30, l: 30, r: 50 },
              barmode: "group",
              autosize: true,
              height: 400,
            }}
            config={{ responsive: true }}
            style={{ width: "100%" }}
          />
        </div>
      </CardContent>
    </Card>
  );
};
