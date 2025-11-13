import React, { useMemo } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { ChevronDown, ChevronRight } from "lucide-react";
import { PlotlyBarChart } from "@/components/charts";


interface SampleCountsProps {
  isOpen: boolean;
  toggleOpen: () => void;
  siteSampleCounts: { site: string; tumor: number; normal: number }[];
  selectedSites: string[];
  selectedGroups: string[];
}


const SampleCounts: React.FC<SampleCountsProps> = ({
  isOpen,
  toggleOpen,
  siteSampleCounts,
  selectedSites,
  selectedGroups
}) => {
  const sampleCountData = useMemo(() => {
    const groupedData: any[] = [];

    selectedSites.forEach((site) => {
      const siteData = siteSampleCounts.find((c) => c.site === site);
      if (!siteData) return;

      if (selectedGroups.includes("tumor")) {
        groupedData.push({
          site,
          type: "Tumor",
          count: siteData.tumor,
          colors: "#ef4445"
        });
      }

      if (selectedGroups.includes("normal")) {
        groupedData.push({
          site,
          type: "Normal",
          count: siteData.normal,
          colors: "#10b981"
        });
      }
    });

    // Pivot into { site, Tumor: X, Normal: Y, colors: ... } objects
    const pivoted: Record<string, any> = {};
    groupedData.forEach((row) => {
      if (!pivoted[row.site]) {
        pivoted[row.site] = { site: row.site };
      }
      pivoted[row.site][row.type] = row.count;
    });

    return Object.values(pivoted);
  }, [siteSampleCounts, selectedSites, selectedGroups]);

  return (
    <div className="mb-8">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-2xl font-bold text-blue-900">
          Sample Count by Site
        </h3>
        <button onClick={toggleOpen} className="text-blue-900">
          {isOpen ? (
            <ChevronDown className="h-6 w-6" />
          ) : (
            <ChevronRight className="h-6 w-6" />
          )}
        </button>
      </div>
      {isOpen && (
        <Card className="border-0 shadow-lg">
          <CardContent className="pt-6">
            <PlotlyBarChart
              data={sampleCountData}
              xKey="site"
              yKey={["Normal", "Tumor" ]} // Multiple keys = multiple bars per site
              title="Sample Counts by Cancer Site"
              xLabel="Cancer Sites"
              yLabel="Number of Samples"
              colors={selectedGroups.map((group) => group === "tumor" ? "#ef4444c3" : "#10b981bd")}
              legendLabels={["Normal", "Tumor"]}
              orientation="v"
            />
          </CardContent>
        </Card>
      )}
    </div>
  );
};



export default SampleCounts;