import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Activity, ArrowLeft, ArrowRight, Download, Loader2, Upload } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";
import GeneSelector from "@/components/GeneSelector";
import Header from "@/components/header";
import Footer from "@/components/footer";
import { useCache } from "@/hooks/use-cache";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import SelectReact from "react-select";               
import {SelectedGene} from  "@/components/types/genes";

const customSelectStyles = {
  control: (provided: any) => ({
    ...provided,
    borderColor: "#e2e8f0",
    "&:hover": { borderColor: "#2563eb" },
    boxShadow: "none",
    backgroundColor: "#fff",
  }),
  option: (provided: any, state: any) => ({
    ...provided,
    backgroundColor: state.isSelected
      ? "#2563eb"
      : state.isFocused
      ? "#f1f5f9"
      : "#fff",
    color: state.isSelected ? "#fff" : "#1e3a8a",
  }),
  menu: (provided: any) => ({
    ...provided,
    border: "1px solid #e2e8f0",
    boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
  }),
};



/* -------------------------------------------------------------------------- */
/*  UploadAnalysis component                                                  */
/* -------------------------------------------------------------------------- */
const UploadAnalysis = () => {
  // const [selectedGenes, setSelectedGenes] = useState<string[]>([]);
  const [selectedGenes, setSelectedGenes] = useState<SelectedGene[]>([]);
  const [expressionFile, setExpressionFile] = useState<File | null>(null);
  const [topN, setTopN] = useState<string>("15");
  const [analysisType, setAnalysisType] = useState<string>("Gene");
  const [geneSet, setGeneSet] = useState<string>("KEGG_2021_Human");

  /* ---- pathway enrichment state (mirrors PathwayAnalysis) ---- */
  const [enrichedPathways, setEnrichedPathways] = useState<
    { id: string; value: string; label: string; category: string; genes?: string[] }[]
  >([]);
  const [selectedPathway, setSelectedPathway] = useState<any>(null);
  const [isLoadingPathways, setIsLoadingPathways] = useState(false);
  const [isFetchingPathwayGenes, setIsFetchingPathwayGenes] = useState(false);
  const [pathwayError, setPathwayError] = useState<string | null>(null);

  const [isLoading, setIsLoading] = useState<boolean>(false);
  const { toast } = useToast();
  const { getCachedData, setCachedData, generateCacheKey } = useCache();
  const navigate = useNavigate();
  
  /* ---------------------------------------------------------------------- */
  /*  1. File handling (unchanged)                                          */
  /* ---------------------------------------------------------------------- */
  
  const handleExpressionFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files[0]) {
      const selectedFile = event.target.files[0];
      if (selectedFile.name.endsWith('.csv') || selectedFile.name.endsWith('.tsv')) {
        setExpressionFile(selectedFile);
      } else {
        toast({
          title: "Invalid File Type",
          description: "Expression data file must be CSV or TSV.",
          variant: "destructive",
        });
        setExpressionFile(null);
      }
    }
  };

  const handleDownloadSample = () => {
    const sampleData = `gene_id,gene_name,SampleA,SampleB
ENSG00000000003.15,TSPAN6,11.9592,21.8393
ENSG00000000005.6,TNMD,0.4836,1.0798
ENSG00000000419.13,DPM1,117.9028,97.2087`;

    const blob = new Blob([sampleData], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'sample_expression_data.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);

    toast({ title: "Sample File Downloaded", description: "Sample expression data file has been downloaded." });
  };

  /* ---------------------------------------------------------------------- */
  /*  2. Pathway enrichment – fetch as soon as genes change                */
  /* ---------------------------------------------------------------------- */
  const fetchEnrichedPathways = async (genes: string[]) => {
    if (!genes.length) {
      setEnrichedPathways([]);
      return;
    }
    setIsLoadingPathways(true);
    setPathwayError(null);
    try {
      const response = await fetch("/api/enriched-pathways", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ genes }),
      });
      if (!response.ok) throw new Error("Failed to fetch enriched pathways");
      const pathways = await response.json();

      const formatted = pathways.map((p: any) => ({
        id: p.id,
        value: p.id,
        label: p.label || p.description || p.id,
        category: p.category || "Unknown",
        genes: p.genes || [],
      }));
      setEnrichedPathways(formatted);
    } catch (err: any) {
      setPathwayError(err.message || "Failed to fetch enriched pathways.");
      setEnrichedPathways([]);
    } finally {
      setIsLoadingPathways(false);
    }
  };

  useEffect(() => {
    if (analysisType === "Pathway" && selectedGenes.length) {
      fetchEnrichedPathways(selectedGenes.map((g) => g.gene_symbol));
    } else {
      setEnrichedPathways([]);
      setSelectedPathway(null);
    }
  }, [selectedGenes, analysisType]);

  /* ---------------------------------------------------------------------- */
  /*  3. When a pathway is selected → load its genes (mirrors PathwayAnalysis) */
  /* ---------------------------------------------------------------------- */
  const handlePathwaySelect = async (option: any) => {
    setSelectedPathway(option || null);
    if (!option) return;

    setIsFetchingPathwayGenes(true);
    try {
      const res = await fetch(`/api/get-genes?pathway=${option.id}`);
      if (!res.ok) throw new Error("Failed to fetch genes for selected pathway.");
      const data = await res.json();
      const genes = data.genes || [];

      setSelectedPathway({ ...option, genes });
      setSelectedGenes(genes.map((g) => ({ gene_symbol: g, ensembl_id: "" })));               // <-- replace gene list
    } catch (e: any) {
      toast({ title: "Error", description: e.message, variant: "destructive" });
    } finally {
      setIsFetchingPathwayGenes(false);
    }
  };

  /* ---------------------------------------------------------------------- */
  /*  4. Form submit (unchanged, only adds pathway data when needed)        */
  /* ---------------------------------------------------------------------- */
  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();

    if (!expressionFile) {
      toast({ title: "No File Uploaded", description: "Please upload an expression data file (CSV or TSV).", variant: "destructive" });
      return;
    }
    if ((analysisType === 'Gene' || analysisType === 'Pathway') && !selectedGenes.length) {
      toast({ title: "No Genes Selected", description: "Please select at least one gene.", variant: "destructive" });
      return;
    }

    setIsLoading(true);

    const cacheParams = {
      genes: selectedGenes.map((g) => g.gene_symbol).sort().join(','),
      expressionFile: expressionFile ? `${expressionFile.name}_${expressionFile.size}` : null,
      analysisType,
      geneSet,
      topN,
      pathwayId: selectedPathway?.id,
    };
    const cacheKey = generateCacheKey(cacheParams);
    const cachedResult = getCachedData(cacheKey);
    if (cachedResult) {
      navigate('/upload-results', { state: { results: cachedResult, analysisType } });
      toast({ title: "Loaded from Cache", description: `Results for ${analysisType} Analysis loaded from cache.` });
      setIsLoading(false);
      return;
    }

    const formData = new FormData();
    if (selectedGenes.length) formData.append('genes', selectedGenes.map((g) => g.gene_symbol).join(','));
    if (expressionFile) {
      formData.append('expression_file', expressionFile);
      formData.append('top_n', topN);
    }
    formData.append('analysis_type', analysisType);
    if (analysisType === 'Pathway') {
      formData.append('gene_set', geneSet);
      if (selectedPathway) {
        formData.append('pathway_id', selectedPathway.id);
        formData.append('pathway_label', selectedPathway.label);
        formData.append('pathway_genes', (selectedPathway.genes ?? []).join('|'));
      }
    }

    try {
      const response = await fetch('/api/csv-upload', { method: 'POST', body: formData });
      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.error || 'Failed to process input');
      }
      const result = await response.json();
      setCachedData(cacheKey, result);
      navigate('/upload-results', { state: { results: result, analysisType } });
      toast({ title: "Upload Successful", description: `Input processed for ${analysisType} Analysis.` });
    } catch (error: any) {
      toast({ title: "Error", description: `Failed to process input: ${error.message}`, variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  /* ---------------------------------------------------------------------- */
  /*  Render                                                                */
  /* ---------------------------------------------------------------------- */
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Link>

          <Card>
            <CardHeader>
              <CardTitle className="text-2xl font-bold text-blue-900">
                Upload Gene Expression Data
              </CardTitle>
            </CardHeader>

            <CardContent className="space-y-6">
              {/* ---------- Analysis Type ---------- */}
              <div>
                <Label className="text-blue-900 block mb-2 font-bold">Analysis Type</Label>
                <div className="border border-gray-300 rounded-lg p-4">
                  <RadioGroup value={analysisType} onValueChange={setAnalysisType} className="flex flex-col space-y-3">
                    {[
                      { value: "Gene", label: "Gene Analysis" },
                      { value: "Pathway", label: "Pathway Analysis" },
                      { value: "Tumor", label: "Tumor Analysis" },
                    ].map((o) => (
                      <div key={o.value} className="flex items-center space-x-2">
                        <RadioGroupItem value={o.value} id={o.value} />
                        <Label htmlFor={o.value} className="text-blue-900">{o.label}</Label>
                      </div>
                    ))}
                  </RadioGroup>
                </div>
              </div>

              {/* ---------- Expression File ---------- */}
              <div>
                <Label htmlFor="expression-file-upload" className="text-blue-900 font-bold">
                  Upload Expression Data File
                </Label>
                <p className="text-sm text-blue-700 mb-2">
                  File must contain 'Hugo_Symbol' or 'gene_name' columns, followed by tumor sample expression data.
                </p>

                {/* Sample table */}
                <div className="overflow-x-auto mb-4 border border-gray-200 rounded-md">
                  <table className="min-w-full text-sm text-left border-collapse">
                    <thead className="bg-gray-100">
                      <tr>
                        <th className="px-4 py-2 border">gene_id</th>
                        <th className="px-4 py-2 border">gene_name</th>
                        <th className="px-4 py-2 border">SampleA</th>
                        <th className="px-4 py-2 border">SampleB</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr><td className="px-4 py-2 border">ENSG00000000003.15</td><td className="px-4 py-2 border">TSPAN6</td><td className="px-4 py-2 border">11.9592</td><td className="px-4 py-2 border">21.8393</td></tr>
                      <tr><td className="px-4 py-2 border">ENSG00000000005.6</td><td className="px-4 py-2 border">TNMD</td><td className="px-4 py-2 border">0.4836</td><td className="px-4 py-2 border">1.0798</td></tr>
                      <tr><td className="px-4 py-2 border">ENSG00000000419.13</td><td className="px-4 py-2 border">DPM1</td><td className="px-4 py-2 border">117.9028</td><td className="px-4 py-2 border">97.2087</td></tr>
                    </tbody>
                  </table>
                </div>

                <div className="flex space-x-4 items-center">
                  <Input
                    id="expression-file-upload"
                    type="file"
                    accept=".csv,.tsv"
                    onChange={handleExpressionFileChange}
                    className="file:mr-4 file:py-0 file:px-6 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                  />
                  <Button type="button" onClick={handleDownloadSample}
                    className="bg-gradient-to-r from-blue-600 to-blue-900 text-white px-3 py-2">
                    <Download className="mr-2 h-3 w-3" />
                    Download Sample File
                  </Button>
                </div>
              </div>

              {/* ---------- Gene selector (always shown for Gene/Pathway) ---------- */}
              {(analysisType === 'Gene' || analysisType === 'Pathway') && (
                <div>
                  {/* <GeneSelector selectedGenes={selectedGenes} onGenesChange={setSelectedGenes} /> */}
                  <GeneSelector
                selectedGenes={selectedGenes}
                onGenesChange={setSelectedGenes}
              />
                </div>
              )}

              {/* ---------- Pathway selector (only for Pathway) ---------- */}
              {analysisType === 'Pathway' && (
                <div>
                  <Label className="text-blue-900 font-bold">Select Enriched Pathway</Label>
                  {isLoadingPathways && <p className="text-sm text-blue-600 mt-1">Loading pathways...</p>}
                  {pathwayError && <p className="text-sm text-red-600 mt-1">{pathwayError}</p>}

                  <SelectReact
                    options={enrichedPathways}
                    onChange={handlePathwaySelect}
                    getOptionLabel={(o) => o.label}
                    getOptionValue={(o) => o.id}
                    placeholder="Search / select a pathway"
                    isClearable
                    isDisabled={isLoadingPathways || isFetchingPathwayGenes}
                    styles={customSelectStyles}
                  />

                  {isFetchingPathwayGenes && (
                    <p className="text-sm text-blue-600 mt-2">Fetching pathway genes...</p>
                  )}
                </div>
              )}

              {/* ---------- Submit ---------- */}
              <form onSubmit={handleSubmit} className="mt-8">
                <div className="flex justify-center py-7">
                  <Button
                    type="submit"
                    disabled={
                      isLoading ||
                      (analysisType === 'Tumor' && !expressionFile) ||
                      ((analysisType === 'Gene' || analysisType === 'Pathway') && !expressionFile && !selectedGenes.length)
                    }
                    className="bg-gradient-to-r from-blue-600 to-blue-900 text-white px-3 py-2"
                  >
                    {isLoading ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Processing...
                      </>
                    ) : (
                      <>
                        <Upload className="mr-2 h-3 w-3" />
                        Analyze Input
                      </>
                    )}
                  </Button>
                </div>
              </form>
            </CardContent>
          </Card>
        </div>
      </main>
      <Footer />
    </div>
  );
};

export default UploadAnalysis;