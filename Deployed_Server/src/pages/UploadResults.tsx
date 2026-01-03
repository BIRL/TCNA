import { useLocation, useNavigate, Link } from 'react-router-dom';
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info, ArrowLeft, Download } from "lucide-react";
import { DataTable } from "@/components/ui/data-table";
import { PlotlyBarChart } from "@/components/charts/PlotlyBarChart";
import PlotlyViolinPlot from "@/components/charts/PlotlyViolinPlot";
import CollapsibleCard from '@/components/ui/collapsible-card';
import Header from "@/components/header";
import Footer from "@/components/footer";
import { Button } from "@/components/ui/button";
import {Card, CardHeader, CardTitle, CardContent  } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import React, { useState, useMemo, useEffect, useCallback, Dispatch, SetStateAction } from 'react';
import { ChevronDown, ChevronRight } from "lucide-react";

// Define interface for GeneAnalysisTable props
interface GeneAnalysisTableProps {
  metricsRaw: any;
  metricsLog2: any;
  topGenesRaw: any;
  topGenesLog2: any;
}

const GeneAnalysisTable = ({ metricsRaw, metricsLog2, topGenesRaw, topGenesLog2 }: GeneAnalysisTableProps) => {
  const [dataFormat, setDataFormat] = useState('log2');

  const handleMetricDataFormatChange = (checked: boolean) => {
    setDataFormat(checked ? 'log2' : 'raw');
  };

  const metrics = dataFormat === 'log2' ? metricsLog2 : metricsRaw;
  const topGenes = dataFormat === 'log2' ? topGenesLog2 : topGenesRaw;

  const columns = [
    { key: 'gene', header: 'Gene', sortable: true },
    { key: 'cv', header: 'CV', sortable: true, render: (value: number) => value.toFixed(4) },
    { key: 'cv_squared', header: 'CV²', sortable: true, render: (value: number) => value.toFixed(4) },
    { key: 'std', header: 'S.D', sortable: true, render: (value: number) => value.toFixed(4) },
    { key: 'mad', header: 'MAD', sortable: true, render: (value: number) => value.toFixed(4) },
    { key: 'mean', header: 'Mean', sortable: true, render: (value: number) => value.toFixed(4) },
  ];

  const data = topGenes.map((gene: string) => ({
    gene,
    cv: metrics.cv?.[gene] || 0,
    cv_squared: metrics.cv_squared?.[gene] || 0,
    std: metrics.std?.[gene] || 0,
    mad: metrics.mad?.[gene] || 0,
    mean: metrics.mean?.[gene] || 0,
  }));

  const downloadCSV = () => {
    const headers = columns.map(col => col.header).join(",");
    const rows = data.map(row => 
      [row.gene, row.cv.toFixed(4), row.cv_squared.toFixed(4), row.std.toFixed(4), row.mad.toFixed(4), row.mean.toFixed(4)].join(",")
    );
    const content = [headers, ...rows].join("\n");
    const blob = new Blob([content], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `gene_analysis_metrics_${dataFormat}_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <CollapsibleCard
      title={`Gene Noise Metrics`}
      className="mb-4"
      downloadButton={
        <Button onClick={downloadCSV} variant="outline" size="sm" className="h-6 px-2 text-xs">
          <Download className="h-4 w-4 mr-2" /> Download CSV
        </Button>
      }
    >
      {/* <div className="flex items-center space-x-2 mb-4 justify-center"> */}
      <div className="flex items-center space-x-2 mb-2 pl-6">
        <Switch
          id="gene-table-data-format-switch"
          checked={dataFormat === 'log2'}
          onCheckedChange={handleMetricDataFormatChange}
          className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
        />
        <Label
          htmlFor="gene-table-data-format-switch"
          className="text-sm text-blue-900"
        >
          Log<sub>2</sub>(X + 1)
        </Label>
      </div>
      <DataTable
        data={data}
        columns={columns}
        defaultSortKey="cv"
        defaultSortOrder="desc"
        className="border rounded-md"
        containerWidth="100%"
      />
    </CollapsibleCard>
  );
};

// Define interface for GeneAnalysisBarChart props
interface GeneAnalysisBarChartProps {
  metricsRaw: any;
  metricsLog2: any;
  topGenesRaw: any;
  topGenesLog2: any;
  selectedGenes: string[];
  setSelectedGenes: Dispatch<SetStateAction<string[]>>;
}

const GeneAnalysisBarChart = ({ metricsRaw, metricsLog2, topGenesRaw, topGenesLog2, selectedGenes }: GeneAnalysisBarChartProps) => {
  const [dataFormatMean, setDataFormatMean] = useState('log2');
  const [dataFormatCV, setDataFormatCV] = useState('log2');

  const handleMetricDataFormatChangeMean = (checked: boolean) => {
    setDataFormatMean(checked ? 'log2' : 'raw');
  };

  const handleMetricDataFormatChangeCV = (checked: boolean) => {
    setDataFormatCV(checked ? 'log2' : 'raw');
  };

  const getData = (dataFormat: string) => {
    const metrics = dataFormat === 'log2' ? metricsLog2 : metricsRaw;
    const topGenes = dataFormat === 'log2' ? topGenesLog2 : topGenesRaw;

    let filteredGenes = topGenes;
    if (selectedGenes.length > 0) {
      filteredGenes = topGenes.filter((gene: string) => selectedGenes.includes(gene));
    }

    return filteredGenes.map((gene: string) => ({
      gene,
      cv: metrics.cv?.[gene] || 0,
      cv_squared: metrics.cv_squared?.[gene] || 0,
      mean_std: metrics.mean?.[gene] || 0,
      mean_mad: metrics.mean?.[gene] || 0,
      std: metrics.std?.[gene] || 0,
      mad: metrics.mad?.[gene] || 0,
    }));
  };

  const dataMean = getData(dataFormatMean);
  const dataCV = getData(dataFormatCV);

  console.log(`GeneAnalysisBarChart Mean Data (${dataFormatMean}):`, dataMean);
  console.log(`GeneAnalysisBarChart CV Data (${dataFormatCV}):`, dataCV);

  return (
    <>
      <CollapsibleCard
        title={`Mean with S.D and MAD`}
        className="mb-4"
      >
        {/* <div className="flex items-center space-x-2 mb-4 justify-center"> */}
        <div className="flex items-center space-x-2 mb-2 pl-6">
          <Switch
            id="gene-chart-data-format-switch"
            checked={dataFormatMean === 'log2'}
            onCheckedChange={handleMetricDataFormatChangeMean}
            className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
          />
          <Label
            htmlFor="gene-chart-data-format-switch"
            className="text-sm text-blue-900"
          >
            Log<sub>2</sub>(X + 1)
          </Label>
        </div>
        <PlotlyBarChart
          data={dataMean}
          title={
            dataFormatMean.toLowerCase() === "log2"
              ? "Mean with S.D and MAD Error Bars (Log\u2082)"
              : `Mean with S.D and MAD Error Bars (Raw)`
          }
          xKey="gene"
          yKey={['mean_std', 'mean_mad']}
          errorKey={['std', 'mad']}
          xLabel="Genes"
          yLabel="Mean Expression"
          colors={['rgba(59, 130, 246, 0.8)', 'rgba(239, 68, 68, 0.8)']}
          legendLabels={['Mean (S.D Error)', 'Mean (MAD Error)']}
          orientation="v"
          showLegend={true}
        />
      </CollapsibleCard>
      <CollapsibleCard
        title={`Coefficient of Variation`}
        className="mb-4"
      >
        <div className="flex items-center space-x-2 mb-2 pl-6">
        {/* <div className="flex items-center space-x-2 mb-4 justify-left"> */}
          <Switch
            id="gene-chart-cv-data-format-switch"
            checked={dataFormatCV === 'log2'}
            onCheckedChange={handleMetricDataFormatChangeCV}
            className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
          />
          <Label
            htmlFor="gene-chart-cv-data-format-switch"
            className="text-sm text-blue-900"
          >
            Log<sub>2</sub>(X + 1)
          </Label>
        </div>
        <PlotlyBarChart
          data={dataCV}
          title={
            dataFormatCV.toLowerCase() === "log2"
              ? "Coefficient of Variation (Log\u2082)"
              : `Coefficient of Variation (Raw)`
          }
          xKey="gene"
          yKey={['cv', 'cv_squared']}
          xLabel="Genes"
          yLabel="Metric Value"
          colors={['rgba(34, 197, 94, 0.6)', 'rgba(151, 34, 197, 0.6)']}
          legendLabels={['CV', 'CV²']}
          orientation="v"
          showLegend={true}
        />
      </CollapsibleCard>
    </>
  );
};

// Define interface for PathwayBarChart props
interface PathwayBarChartProps {
  heatmapDataRaw: any;
  heatmapDataLog2: any;
  topGenesRaw: any;
  topGenesLog2: any;
  selectedGenes: string[];
  setSelectedGenes: Dispatch<SetStateAction<string[]>>;
}

const PathwayBarChart = ({ heatmapDataRaw, heatmapDataLog2, topGenesRaw, topGenesLog2, selectedGenes }: PathwayBarChartProps) => {
  const [dataFormat, setDataFormat] = useState('log2');

  const handleMetricDataFormatChange = (checked: boolean) => {
    setDataFormat(checked ? 'log2' : 'raw');
  };

  const heatmapData = dataFormat === 'log2' ? heatmapDataLog2 : heatmapDataRaw;
  const topGenes = dataFormat === 'log2' ? topGenesLog2 : topGenesRaw;

  const data = (selectedGenes.length > 0 
    ? topGenes.filter((gene: string) => selectedGenes.includes(gene))
    : topGenes
  ).map((gene: string) => ({
    gene,
    mean: heatmapData[gene]?.mean || 0,
    cv: heatmapData[gene]?.cv || 0,
  }));

  return (
    <CollapsibleCard title={`Pathway Analysis (Mean and CV)`} className="mb-4">
      {/* <div className="flex items-center space-x-2 mb-4 justify-center"> */}
      <div className="flex items-center space-x-2 mb-2 pl-6">
        <Switch
          id="pathway-chart-data-format-switch"
          checked={dataFormat === 'log2'}
          onCheckedChange={handleMetricDataFormatChange}
          className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
        />
        <Label
          htmlFor="pathway-chart-data-format-switch"
          className="text-sm text-blue-900"
        >
          Log<sub>2</sub>(X + 1)
        </Label>
      </div>
      <PlotlyBarChart
        data={data}
        title={
          dataFormat.toLowerCase() === "log2"
            ? "Mean and CV of Gene Expression (Log\u2082)"
            : `Mean and CV of Gene Expression (Raw)`
        }
        xKey="gene"
        yKey={['mean', 'cv']}
        xLabel="Genes"
        yLabel="Value"
        colors={['rgba(59, 130, 246, 0.6)', 'rgba(239, 68, 68, 0.6)']}
        legendLabels={['Mean Expression', 'Coefficient of Variation']}
        orientation="v"
        showLegend={true}
      />
    </CollapsibleCard>
  );
};

// Define interface for GeneSelectionSidebar props
interface GeneSelectionSidebarProps {
  topGenes: string[];
  selectedGenes: string[];
  setSelectedGenes: Dispatch<SetStateAction<string[]>>;
}


const GeneSelectionSidebar = ({ topGenes, selectedGenes, setSelectedGenes }: GeneSelectionSidebarProps) => {
  const [isOpen, setIsOpen] = useState(true);

  const toggleSection = useCallback(() => {
    setIsOpen(prev => !prev);
  }, []);

  const handleGeneSelection = (gene: string, checked: boolean) => {
    setSelectedGenes(prev => 
      checked ? [...prev, gene] : prev.filter(g => g !== gene)
    );
  };

  const allSelected = selectedGenes.length === topGenes.length;
  const someSelected = selectedGenes.length > 0 && !allSelected;

  const handleSelectAll = (checked: boolean) => {
    if (checked) {
      setSelectedGenes(topGenes); // select all
    } else {
      setSelectedGenes([]); // clear all
    }
  };

  return (
    <div className="w-72 bg-blue-100 shadow-lg rounded-lg sticky top-24 self-start">
      <Card className="border-0 shadow-lg bg-blue-100">
        <CardHeader className="pb-2">
          <div className="flex justify-between items-center">
            <CardTitle className="text-lg font-bold text-blue-900">Gene Selection</CardTitle>
            <button onClick={toggleSection} className="text-blue-900">
              {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            </button>
          </div>
        </CardHeader>
        {isOpen && (
          <CardContent className="space-y-2">
            <div className="flex items-center space-x-2">
              <Checkbox
                id="gene-select-all"
                checked={allSelected}
                onCheckedChange={(checked) => handleSelectAll(checked as boolean)}
                className={someSelected ? "data-[state=indeterminate]:bg-blue-400" : ""}
              />
              <Label
                htmlFor="gene-select-all"
                className="text-sm font-bold text-blue-900"
              >
                Select All
              </Label>
            </div>
            <div className="space-y-2">
              {topGenes.map((gene: string) => (
                <div key={gene} className="flex items-center space-x-2">
                  <Checkbox
                    id={`gene-select-${gene}`}
                    checked={selectedGenes.includes(gene)}
                    onCheckedChange={(checked) => handleGeneSelection(gene, checked as boolean)}
                  />
                  <Label
                    htmlFor={`gene-select-${gene}`}
                    className="text-sm text-blue-900"
                  >
                    {gene}
                  </Label>
                </div>
              ))}
            </div>
          </CardContent>
        )}
      </Card>
    </div>
  );
};

const TumorSidebar = () => {
  const [isOpen, setIsOpen] = useState(true);

  const toggleSection = useCallback(() => {
    setIsOpen(prev => !prev);
  }, []);

  return (
    <div className="w-72 bg-white-100 shadow-lg rounded-lg sticky top-24 self-start">
      <Card className="border-0 shadow-lg bg-blue-100">
        <CardHeader className="pb-2">
          <div className="flex justify-between items-center">
            <CardTitle className="text-lg font-bold text-blue-900">ITH Metrics</CardTitle>
            <button onClick={toggleSection} className="text-blue-900">
              {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            </button>
          </div>
        </CardHeader>
        {isOpen && (
          <CardContent className="space-y-4 text-sm text-blue-900">
            <div>
              
              <h4 className="text-sm font-bold text-blue-900">DEPTH2</h4>
              <p className="mb-2">
                DEPTH2 calculates a tumor’s ITH level based on the standard deviations of absolute z-scored expression values.
              </p>
              <a
                href="https://pmc.ncbi.nlm.nih.gov/articles/PMC8974098/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-700"
              >
                Learn more here
              </a>
            </div>
            <div>
              <h4 className="text-sm font-bold text-blue-900">DEPTH</h4>
              <p className="mb-2">
                DEPTH calculates a tumor’s ITH level with reference to normal controls.
              </p>
              <a
                href="https://www.nature.com/articles/s42003-020-01230-7"
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-700"
              >
                Learn more here
              </a>
            </div>
          </CardContent>
        )}
      </Card>
    </div>
  );
};

const TumorAnalysisTable = ({ metrics }) => {
  const columns = [
    { key: 'sample', header: 'Sample', sortable: true },
    { key: 'DEPTH2', header: 'DEPTH2', sortable: true, render: (value: number) => value.toFixed(4)},
    { key: 'DEPTH', header: 'DEPTH', sortable: true, render: (value: number) => value.toFixed(4) },
  ];

  const downloadCSV = () => {
    const headers = columns.map(col => col.header).join(",");
    const rows = metrics.map(row => 
      [row.sample, row.DEPTH2.toFixed(4), row.DEPTH.toFixed(4)].join(",")
    );
    const content = [headers, ...rows].join("\n");
    const blob = new Blob([content], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `tumor_metrics_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <CollapsibleCard
      title="TH Metrics Table"
      className="mb-4"
      downloadButton={
        <Button onClick={downloadCSV} variant="outline" size="sm" className="h-6 px-2 text-xs">
          <Download className="h-4 w-4 mr-2" /> Download CSV
        </Button>
      }
    >
      <DataTable
        data={metrics}
        columns={columns}
        defaultSortKey="DEPTH2"
        defaultSortOrder="desc"
        className="border rounded-md"
        containerWidth="100%"
      />
    </CollapsibleCard>
  );
};

const TumorAnalysisBoxPlot = ({ metrics }) => {
  const data = metrics.map(item => ({
    metric: 'DEPTH2',
    value: item.DEPTH2,
  })).concat(metrics.map(item => ({
    metric: 'DEPTH',
    value: item.DEPTH,
  })));

  return (
    <CollapsibleCard title="TH Metrics Plots" className="mb-4">
      <div className="space-y-6">
        <PlotlyViolinPlot
          data={data}
          title="Distribution of Tumor Metrics"
          xKey="metric"
          yLabel="Metric Value"
          normalizationMethod="Metric Value"
          selectedGroups={['DEPTH2', 'DEPTH']}
          colorMap={{ 'DEPTH2': 'hsl(200, 70%, 50%)', 'DEPTH': 'hsla(301, 98%, 32%, 1.00)' }}
          className="border rounded-md"
          showLegend={true}
        />
      </div>
    </CollapsibleCard>
  );
};



const PathwayAnalysisTable = ({ enrichmentRaw, enrichmentLog2 }) => {
  const [dataFormat, setDataFormat] = useState('log2');

  const handleMetricDataFormatChange = (checked: boolean) => {
    setDataFormat(checked ? 'log2' : 'raw');
  };

  const enrichment = dataFormat === 'log2' ? enrichmentLog2 : enrichmentRaw;

  const columns = [
    { key: 'Term', header: 'Pathway', sortable: true },
    { key: 'FDR', header: 'FDR', sortable: true, render: (value) => value.toExponential(2) },
    { key: 'Genes', header: 'Genes', render: (value) => value.join(', ') },
    { key: 'GeneSet', header: 'Gene Set' },
  ];

  const downloadCSV = () => {
    const headers = columns.map(col => col.header).join(",");
    const rows = enrichment.map(row => 
      [row.Term, row['FDR'].toExponential(4), row.Genes.join(", "), row.GeneSet || ""].join(",")
    );
    const content = [headers, ...rows].join("\n");
    const blob = new Blob([content], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pathway_enrichment_${dataFormat}_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <CollapsibleCard
      title={`Enriched Pathways`}
      className="mb-4"
      downloadButton={
        <Button onClick={downloadCSV} variant="outline" size="sm" className="h-6 px-2 text-xs">
          <Download className="h-4 w-4 mr-2" /> Download CSV
        </Button>
      }
    >
      {/* <div className="flex justify-end items-center space-x-2 mb-4"> */}
      {/* <div className="flex items-center space-x-2 mb-4 justify-center">
        <Switch
          id="pathway-table-data-format-switch"
          checked={dataFormat === 'log2'}
          onCheckedChange={handleMetricDataFormatChange}
          className="data-[state=checked]:bg-blue-600 data-[state=unchecked]:bg-gray-300"
        />
        <Label
          htmlFor="pathway-table-data-format-switch"
          className="text-sm text-blue-900"
        >
          Log<sub>2</sub>(X + 1)
        </Label>
      </div> */}
      <DataTable
        data={enrichment}
        columns={columns}
        defaultSortKey="FDR"
        defaultSortOrder="asc"
        className="border rounded-md"
        containerWidth="100%"
      />
    </CollapsibleCard>
  );
};

const UploadResults = () => {
  const { state } = useLocation();
  const navigate = useNavigate();
  const { results, analysisType } = state || {};
  const [selectedGenes, setSelectedGenes] = useState<string[]>([]);
  useEffect(() => {
    if ((analysisType === 'Gene' || analysisType === 'Pathway') && results?.log2?.top_genes) {
      setSelectedGenes(results.log2.top_genes);
    }
  }, [analysisType, results]);
  useEffect(() => {
      window.scrollTo(0, 0);
    }, []);

  if (!results || !analysisType) {
    return (
      <div className="min-h-screen bg-white flex flex-col">
        <Header />
        <main className="flex-grow py-8">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <Card className="shadow-lg p-6 text-center">
              <h2 className="text-2xl font-bold text-blue-900 mb-4">No Results
                <p className="text-blue-700 mb-6">Please submit an analysis from the upload page to view results.</p>
                <Button
                  onClick={() => navigate('/')}
                  className="bg-gradient-to-r from-blue-600 to-blue-900 text-white px-3 py-2"
                >
                  Return to Upload Page
                </Button>
              </h2>
            </Card>
          </div>
        </main>
        <Footer />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-yellow-50 flex flex-col">
      <Header />
      <main className="flex-grow">
  <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
    {/* Fix the grid structure */}
    <div className="grid grid-cols-1 lg:grid-cols-[288px_1fr] gap-8 items-start">
          {/* Sidebar always on the left */}
          {(analysisType === 'Gene' || analysisType === 'Pathway') && results.log2 && (
            <GeneSelectionSidebar
              topGenes={results.log2.top_genes}
              selectedGenes={selectedGenes}
              setSelectedGenes={setSelectedGenes}
            />
          )}
          {analysisType === 'Tumor' && (
              <TumorSidebar />
            )}
                <div className="flex-1 min-w-0">
            <div className="flex-grow">
              <Link
                to="/upload-analysis"
                className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-4 transition-colors"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back to Upload
              </Link>
              <div className="mb-8">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-4xl font-bold text-blue-900">Results For {analysisType} Analysis</h2>
                </div>
                <div className="overflow-x-auto mb-6">
                  <table className="min-w-full bg-white border border-gray-200 rounded-lg shadow-sm">
                    <tbody>
                      <tr className="border-b">
                        <th className="text-left py-3 px-4 text-blue-700 font-semibold w-1/3">Analysis Type</th>
                        <td className="py-3 px-4 text-blue-700">{analysisType}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                {(results.raw?.warning || results.log2?.warning) && (
                  <Card className="shadow-lg p-4 mb-4 text-center text-yellow-700 bg-red-50">
                    {results.raw?.warning && results.log2?.warning ? (
                      <p className="text-lg">{results.raw.warning}</p>
                    ) : results.raw?.warning ? (
                      <p className="text-lg">{results.raw.warning}</p>
                    ) : results.log2?.warning ? (
                      <p className="text-lg">{results.log2.warning}</p>
                    ) : null}
                  </Card>
                )}
              </div>
               {analysisType === 'Gene' && results.raw && results.log2 && (
                <>
                  {selectedGenes.length === 0 ? (
                    <Alert className="mb-6 border-yellow-400 bg-yellow-50 text-yellow-800">
                      <AlertDescription>
                        Please select at least one gene from the left panel to view analysis results.
                      </AlertDescription>
                    </Alert>
                  ) : (
                    <>
                      <GeneAnalysisBarChart
                        metricsRaw={results.raw.metrics}
                        metricsLog2={results.log2.metrics}
                        topGenesRaw={results.raw.top_genes}
                        topGenesLog2={results.log2.top_genes}
                        selectedGenes={selectedGenes}
                        setSelectedGenes={setSelectedGenes}
                      />
                      <GeneAnalysisTable
                        metricsRaw={results.raw.metrics}
                        metricsLog2={results.log2.metrics}
                        topGenesRaw={results.raw.top_genes}
                        topGenesLog2={results.log2.top_genes}
                      />
                    </>
                  )}
                </>
              )}
              {analysisType === 'Pathway' && results.raw && results.log2 && (
                <>
                  {selectedGenes.length === 0 ? (
                    <Alert className="mb-6 border-yellow-400 bg-yellow-50 text-yellow-800">
                      <AlertDescription>
                        Please select at least one gene from the left panel to view analysis results.
                      </AlertDescription>
                    </Alert>
                  ) : (
                    <>
                      
                      {results.raw.heatmap_data && results.raw.top_genes &&
                      results.log2.heatmap_data && results.log2.top_genes && (
                        <PathwayBarChart
                          heatmapDataRaw={results.raw.heatmap_data}
                          heatmapDataLog2={results.log2.heatmap_data}
                          topGenesRaw={results.raw.top_genes}
                          topGenesLog2={results.log2.top_genes}
                          selectedGenes={selectedGenes}
                          setSelectedGenes={setSelectedGenes}
                        />
                      )}{results.raw.enrichment?.length > 0 && results.log2.enrichment?.length > 0 && (
                        <PathwayAnalysisTable
                          enrichmentRaw={results.raw.enrichment}
                          enrichmentLog2={results.log2.enrichment}
                        />
                      )}
                    </>
                  )}
                </>
              )}
              {analysisType === 'Tumor' && results.log2?.metrics && (
                <>
                  <TumorAnalysisBoxPlot metrics={results.log2.metrics} />
                  <TumorAnalysisTable metrics={results.log2.metrics} />
                </>
              )}
            </div>
          </div>
        </div>
        </div>
      </main>
      <Footer />
    </div>
  );
};

export default UploadResults;