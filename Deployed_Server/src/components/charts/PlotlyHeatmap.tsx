import React, { useEffect, useRef } from "react";
import Plotly from "plotly.js-dist-min";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

interface PlotlyHeatmapProps {
  title: string;
  data: any[];
  xValues: string[];
  yValues: string[];
  zValues: number[][];
  zLabel: string;
  chartKey: string;
  xLabel?: string;
  yLabel?: string;
  gapcolor?: string;
  colorscale?: string | Array<[number, string]>;
  annotation?: string;
  height?: number;
  className?: string;
}

export const PlotlyHeatmap: React.FC<PlotlyHeatmapProps> = ({
  title,
  data,
  xValues,
  yValues,
  zValues,
  zLabel,
  chartKey,
  xLabel = "X Axis",
  yLabel = "Y Axis",
  colorscale,
  gapcolor,
  annotation,
  height = 400,
  className = "",
}) => {
  const plotRef = useRef<HTMLDivElement>(null);

  // Only affects the heatmap colors, not layout
  const neutralBlueYellowScale: Array<[number, string]> = [
  [0.0, "#1b2a41"], // deep navy blue
  [0.25, "#324b66"], // slate blue-gray
  [0.5, "#b8b8b8"], // neutral gray midpoint
  [0.75, "#d9c97c"], // muted sand yellow
  [1.0, "#f4e34f"], // bright pastel yellow highlight
];

  useEffect(() => {
    if (plotRef.current && data.length > 0) {
      const plotData: Partial<Plotly.Data>[] = [
        {
          x: xValues,
          y: yValues,
          z: zValues,
          type: "heatmap",
          colorscale: colorscale || neutralBlueYellowScale,
          showscale: true,
          colorbar: {
            title: zLabel,
            tickfont: { color: "#333" },
            titlefont: { color: "#333" },
          },
        },
      ];

      const layout: Partial<Plotly.Layout> = {
        // Keep background neutral — no tint
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",

        title: {
          text: title,
          x: 0.55,
          xanchor: "center",
          font: { size: 16, color: "#000000ff", weight: "bold" },
        },
        xaxis: {
          title: xLabel,
          
          tickangle: -25,
          titlefont: { size: 12, weight: "bold" },
          tickfont: { size: 10, weight: "bold" },
          automargin: true,
          linecolor: "black",  // make y-axis line black  
        },
        yaxis: {
          title: yLabel,
          titlefont: { size: 12 },
          tickfont: { size: 10, weight: "bold" },
          automargin: true,
          linecolor: "black",  // make y-axis line black  
        },
        annotations: annotation
          ? [
              {
                text: annotation,
                xref: "paper",
                yref: "paper",
                x: 0.5,
                y: -0.25,
                showarrow: false,
                font: {
                  size: 12,
                  color: "black",
                  weight: "bold"
                },
              },
            ]
          : [],
        // margin: { t: 60, b: annotation ? 80 : 60, l: 100, r: 20 },
        // height,
        margin: { t: 50, b: 80, l: 30, r: 50 },
        width: 850,
        height: 400,
      };


      // Force a neutral theme override to remove residual tints
      Plotly.newPlot(plotRef.current, plotData, layout, {
        responsive: true,
        displayModeBar: true,
      }).then(() => {
        Plotly.relayout(plotRef.current!, {
          "template.layout.paper_bgcolor": "#ffffff",
          "template.layout.plot_bgcolor": "#ffffff",
        });
      });

      const handleResize = () => {
        if (plotRef.current) {
          Plotly.Plots.resize(plotRef.current);
        }
      };
      window.addEventListener("resize", handleResize);
      handleResize();

      return () => {
        window.removeEventListener("resize", handleResize);
      };
    }
  }, [
    data,
    xValues,
    yValues,
    zValues,
    zLabel,
    title,
    xLabel,
    yLabel,
    colorscale,
    annotation,
    height,
  ]);

  return (
    <div className={className}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center space-x-2">
          {/* <span>{title}</span> */}
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            if (plotRef.current) {
              Plotly.downloadImage(plotRef.current, {
                filename: `${title.replace(/\s+/g, "_").toLowerCase()}_${Date.now()}`,
                format: "png",
                width: 800,
                height,
              });
            }
          }}
          className="h-6 px-2 text-xs"
        >
          <Download className="h-3 w-3"  /> Download Plot
        </Button>
      </div>
      <div ref={plotRef} className="chart-container" />
    </div>
  );
};