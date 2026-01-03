import React, { useEffect, useRef } from "react";
import Plotly from "plotly.js-dist-min";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

interface PlotlyBoxChartProps {
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

const PlotlyBoxPlot: React.FC<PlotlyBoxChartProps> = ({
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
    const groupData = data.filter((d) => d.cancer_type === group && d[xKey] != null);
    return {
      y: groupData.map((d) => d[xKey]),
      text: groupData.map((d) => d.sample),
      name: group,
      type: "violin",
      // points: "all", 
      box: {
      visible: true,
    },
      marker: {
        color: colorMap[group] || undefined,
        size: 8,
      },
    };
  });

  const layout: Partial<Plotly.Layout> = {
    title: {
      text: title,
      x: 0.5,
      xanchor: "center",
      font: { size: 16, color: "#000", weight: "bold" },
    },
    boxmode: "group",
    margin: { t: 40, l: 60, r: 20, b: 60 },
    yaxis: {
    title: {
      text: yLabel,
      font: { size: 14, color: "#000", weight: "bold" }, // valid properties only
      // standoff: 20 // space between axis and title
    },
    tickfont: { size: 10, weight: "bold", pad: 20 },
    linecolor: "black",  // make y-axis line black  
      zeroline: false,
      // rangemode: "tozero", 
      automargin: true

    },
    xaxis: {
      title: {
      font: { size: 12, color: "#000", weight: "bold" }, // valid properties only
      standoff: 20, // space between axis and title
    },
    tickfont: { size: 12, weight: "bold", pad: 15 },
    // linecolor: "black",  
    // automargin: true,
    // zeroline: false
      showline: true,
      linecolor: "black",
      // linewidth: 2,
      mirror: false,   // keeps only the baseline, not the top axis line
      anchor: "y",

      // linecolor: "black"
    },
    paper_bgcolor: "white",
    plot_bgcolor: "white",
    showlegend: showLegend,
    legend: {
      orientation: "v",
      x: 0.05,
      xanchor: "center",
      y: 1.2,           // position above plot area, but under title
      yanchor: "bottom", // anchor the legend from the bottom
    },
    autosize: true,
  };

  // Ensure Plotly renders into the div and is ready before download
  Plotly.newPlot(plotRef.current, groupedData, layout, { responsive: true }).then(() => {
    Plotly.Plots.resize(plotRef.current!);
  });

  const handleResize = () => {
    if (plotRef.current) Plotly.Plots.resize(plotRef.current);
  };
  window.addEventListener("resize", handleResize);

  return () => window.removeEventListener("resize", handleResize);
}, [data, selectedGroups, xKey, colorMap, title]);


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

export default PlotlyBoxPlot;