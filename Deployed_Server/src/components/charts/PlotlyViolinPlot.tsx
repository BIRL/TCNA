import React, { useEffect, useRef } from "react";
import Plotly from "plotly.js-dist-min";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

interface PlotlyViolinPlotProps {
  data: any[];
  title: string;
  xKey: string;
  yLabel?: string;
  normalizationMethod: string;
  selectedGroups: string[];
  colorMap?: { [key: string]: string };
  className?: string;
  showLegend?: boolean;
}

const PlotlyViolinPlot: React.FC<PlotlyViolinPlotProps> = ({
  data,
  title,
  xKey,
  yLabel,
  normalizationMethod,
  selectedGroups,
  colorMap = {},
  className = "",
  showLegend = true
}) => {
  const plotRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!plotRef.current || data.length === 0) return;

    const groupedData = selectedGroups.map((group) => {
      const groupData = data.filter((d) => d[xKey] === group && d.value != null);
      const values = groupData.map((d) => d.value);

      return {
        x: values.map(() => group), // Repeat x value for each data point
        y: values,
        type: "violin",
        points: "all", 
        name: group,
        box: { visible: true },
        meanline: { visible: true },
        // line: { color: 'green', width: 1 },
        marker: { color: colorMap[group] || '#2577b2ff' },
        // points: false,
        hoverinfo: 'y+name',
      };
    });

    const layout: Partial<Plotly.Layout> = {
      title: {
        text: title,
        x: 0.5,
        xanchor: "center",
        font: { size: 16, color: "#000", weight: "bold" },
      },
      xaxis: {
        title: {
          text: "Metrics",
          font: { size: 14, color: "#000000ff", weight: "bold" },
          standoff: 20,
        },
        tickfont: { size: 12, weight: "bold", pad: 15 },
        showline: true,
        linecolor: "black",
        mirror: false,
        anchor: "y",
      },
      yaxis: {
        title: {
          text: yLabel || normalizationMethod,
          font: { size: 14, color: "#000", weight: "bold" },
          standoff: 20,
        },
        tickfont: { size: 10, weight: "bold", pad: 20 },
        linecolor: "black",
        zeroline: false,
        automargin: true,
      },
      showlegend: showLegend,
      legend: {
        orientation: "v",
        x: 0.05,
        xanchor: "center",
        y: 1.2,
        yanchor: "bottom",
      },
      paper_bgcolor: "white",
      plot_bgcolor: "white",
      margin: { t: 40, l: 60, r: 20, b: 60 },
      autosize: true,
    };

    Plotly.newPlot(plotRef.current, groupedData, layout, { responsive: true }).then(() => {
      Plotly.Plots.resize(plotRef.current!);
    });

    const handleResize = () => {
      if (plotRef.current) Plotly.Plots.resize(plotRef.current);
    };
    window.addEventListener("resize", handleResize);

    return () => window.removeEventListener("resize", handleResize);
  }, [data, selectedGroups, xKey, colorMap, title, yLabel, normalizationMethod]);

  const handleDownload = () => {
    if (plotRef.current) {
      Plotly.downloadImage(plotRef.current, {
        filename: `${title.replace(/\s+/g, "_").toLowerCase()}_${Date.now()}`,
        format: "png",
        width: 800,
        height: 400,
      });
    }
  };

  return (
    <div className={className}>
      <div className="flex items-center justify-between mb-2">
        <div />
        <Button
          size="sm"
          variant="outline"
          onClick={handleDownload}
          className="h-6 px-2 text-xs"
        >
          <Download className="h-3 w-3 mr-1" />
          Download Plot
        </Button>
      </div>
      <div ref={plotRef} className="w-full h-[400px]" />
    </div>
  );
};

export default PlotlyViolinPlot;