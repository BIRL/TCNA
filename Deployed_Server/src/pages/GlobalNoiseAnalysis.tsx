'use client';

import React, { useState, useCallback, useEffect } from "react";
import { Link } from "react-router-dom";
import { ArrowLeft, Download, TrendingUp, AlertCircle, Search, X, Activity } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { DataTable } from "@/components/ui/data-table";
import { LoadingSpinner } from "@/components/ui/loadingSpinner";
import Header from "@/components/header";
import Footer from "@/components/footer";
import CancerTypeSelector from "@/components/siteSelector";
import { useCache } from "@/hooks/use-cache";
import { PlotlyBarChart } from "@/components/charts/PlotlyBarChart";

interface NoisyGene {
  gene_symbol: string;
  cv_tumor: number;
  cv_normal: number | null;
  site_name?: string;
}

interface EnrichmentResult {
  id: string;
  label: string;
  description?: string;
  category: string;
  genes: string[];
  fdr?: number;
  source?: string;
}

const GlobalNoiseAnalysis: React.FC = () => {
  const [scope, setScope] = useState<"global" | "site-specific">("global");
  const [selectedSites, setSelectedSites] = useState<string[]>([]);
  const [selectedCancerTypes, setSelectedCancerTypes] = useState<string[]>([]);
  const [topN, setTopN] = useState<number>(20);
  const [normMethod, setNormMethod] = useState<"tpm" | "fpkm" | "fpkm_uq">("tpm");

  const [tumorGenes, setTumorGenes] = useState<NoisyGene[]>([]);
  const [normalGenes, setNormalGenes] = useState<NoisyGene[]>([]);
  const [hasNormalData, setHasNormalData] = useState(false);

  // Enrichment states
  const [enrichment, setEnrichment] = useState<EnrichmentResult[]>([]);
  const [filteredEnrichment, setFilteredEnrichment] = useState<EnrichmentResult[]>([]);
  const [selectedPathway, setSelectedPathway] = useState<EnrichmentResult | null>(null);
  const [searchTerm, setSearchTerm] = useState("");

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();

  // Filter pathways when search changes
  useEffect(() => {
    if (!enrichment.length) {
      setFilteredEnrichment([]);
      return;
    }
    const filtered = enrichment.filter(p =>
      (p.description || p.label || "").toLowerCase().includes(searchTerm.toLowerCase()) ||
      p.category.toLowerCase().includes(searchTerm.toLowerCase()) ||
      p.genes.some(g => g.toLowerCase().includes(searchTerm.toLowerCase()))
    );
    setFilteredEnrichment(filtered);
  }, [enrichment, searchTerm]);

  const runAnalysis = useCallback(async () => {
    if (scope === "site-specific" && selectedSites.length === 0) {
      setError("Please select at least one cancer site.");
      return;
    }

    setIsLoading(true);
    setError(null);
    setTumorGenes([]);
    setNormalGenes([]);
    setHasNormalData(false);
    setEnrichment([]);
    setSelectedPathway(null);
    setSearchTerm("");

    const cacheKey = generateCacheKey({
      endpoint: "global-noise-enrichment",
      scope,
      sites: selectedSites,
      topN,
      norm: normMethod,
    });

    const cached = getCachedData(cacheKey);
    if (cached) {
      setTumorGenes(cached.tumorGenes || []);
      setNormalGenes(cached.normalGenes || []);
      setHasNormalData(cached.hasNormalData || false);
      setEnrichment(cached.enrichment || []);
      setIsLoading(false);
      return;
    }

    try {
      const params = new URLSearchParams({
        norm: normMethod,
        top_n: topN.toString(),
      });
      if (scope === "site-specific" && selectedSites.length > 0) {
        params.append("sites", selectedSites.join(","));
      }
      if (scope === "site-specific" && selectedCancerTypes.length > 0) {
        params.append("cancer_types", selectedCancerTypes.join(","));
      }

      const res = await fetch(`/api/top-noisy-genes?${params}`);
      if (!res.ok) throw new Error(`API Error: ${await res.text()}`);
      const data = await res.json();

      const genes: NoisyGene[] = (data.genes || []).map((g: any) => ({
        gene_symbol: g.gene_symbol,
        cv_tumor: g.cv_tumor,
        cv_normal: g.cv_normal,
        site_name: g.site_name || (scope === "global" ? "Pan-Cancer" : undefined),
      }));

      const tumorRanked = genes.filter(g => g.cv_tumor != null).sort((a, b) => b.cv_tumor - a.cv_tumor);
      const normalRanked = genes.filter(g => g.cv_normal != null).sort((a, b) => (b.cv_normal || 0) - (a.cv_normal || 0));

      setTumorGenes(tumorRanked);
      setNormalGenes(normalRanked);
      setHasNormalData(normalRanked.length > 0);

      // Run pathway enrichment on top tumor genes
      if (tumorRanked.length > 0) {
        const geneList = tumorRanked.slice(0, Math.min(500, tumorRanked.length)).map(g => g.gene_symbol);

        try {
          const enrichRes = await fetch("/api/enriched-pathways", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ genes: geneList }),
          });

          if (enrichRes.ok) {
            const enrichData: any[] = await enrichRes.json();
            const formatted = enrichData.map(item => ({
              id: item.id || item.term_id || String(Math.random()),
              label: item.label || item.name || item.term,
              description: item.description || item.name || item.label,
              category: item.category || item.source || "Unknown",
              genes: Array.isArray(item.genes) ? item.genes : item.overlapping_genes || [],
              fdr: item.fdr || item.p_adjusted || item.qvalue || 1,
              source: item.source || "Enrichr",
            }));

            setEnrichment(formatted);
            setCachedData(cacheKey, {
              tumorGenes: tumorRanked,
              normalGenes: normalRanked,
              hasNormalData: normalRanked.length > 0,
              enrichment: formatted,
            });
          }
        } catch (enrichErr) {
          console.warn("Enrichment failed (optional)", enrichErr);
          // Don't break the whole analysis
        }
      } else {
        setCachedData(cacheKey, {
          tumorGenes: tumorRanked,
          normalGenes: normalRanked,
          hasNormalData: normalRanked.length > 0,
          enrichment: [],
        });
      }

    } catch (err: any) {
      setError(err.message || "Analysis failed");
    } finally {
      setIsLoading(false);
    }
  }, [scope, selectedSites, topN, normMethod, getCachedData, setCachedData, generateCacheKey]);

  const downloadCSV = (type: "tumor" | "normal") => {
    const data = type === "tumor" ? tumorGenes : normalGenes;
    const suffix = type === "tumor" ? "tumor" : "normal";
    const headers = ["Rank", "Gene", `CV (${type === "tumor" ? "Tumor" : "Normal"})`, "Site"];
    const rows = data.map((g, i) => [
      (i + 1).toString(),
      g.gene_symbol,
      type === "tumor" ? g.cv_tumor.toFixed(6) : (g.cv_normal || "NA"),
      g.site_name || "Pan-Cancer",
    ]);
    const csv = [headers.join(","), ...rows.map(r => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `top_${topN}_noisy_genes_${suffix}_${scope}_${normMethod}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const getScopeLabel = () => scope === "global" ? "Pan-Cancer" : `${selectedSites.length} Site(s)`;

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-800 mb-4 text-sm">
            <ArrowLeft className="h-4 w-4 mr-2" /> Back to Dashboard
          </Link>
          <h1 className="text-4xl font-bold text-blue-900 mb-4">
            Top Noisy Genes 
          </h1>

          <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
            {/* Controls */}
            <Card className="shadow-xl lg:col-span-1 h-fit lg:sticky lg:top-4">
              <CardHeader>
                <CardTitle className="text-xl text-blue-900">Settings</CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div>
                  <Label>Scope</Label>
                  <Select value={scope} onValueChange={(v: any) => { setScope(v); setSelectedSites([]); }}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="global">Pan-Cancer</SelectItem>
                      <SelectItem value="site-specific">By Cancer Site</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {scope === "site-specific" && (
                  <div>
                    <Label>Cancer Sites</Label>
                    <CancerTypeSelector
                      selectedSites={selectedSites}
                      onSitesChange={setSelectedSites}
                      selectedCancerTypes={selectedCancerTypes}   
                      // CancerTypes={selectedCancerTypes}
                      onCancerTypesChange={setSelectedCancerTypes}
                      analysisType="cancer-specific"
                    />
                  </div>
                )}

                <div>
                  <Label>Normalization</Label>
                  <Select value={normMethod} onValueChange={(v: any) => setNormMethod(v)}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="tpm">TPM</SelectItem>
                      <SelectItem value="fpkm">FPKM</SelectItem>
                      <SelectItem value="fpkm_uq">FPKM_UQ</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label>Top N Genes</Label>
                  <Input
                    type="number"
                    value={topN}
                    step={100}
                    onChange={(e) => setTopN(Math.max(50, Math.min(300, parseInt(e.target.value) || 500)))}
                  />
                </div>

                <Button
                  onClick={runAnalysis}
                  disabled={isLoading}
                  className="w-full bg-gradient-to-r from-blue-600 to-indigo-700 hover:from-blue-700 hover:to-indigo-800"
                  size="lg"
                >
                  {isLoading ? "Running Analysis..." : <> Run Analysis</>}
                </Button>
              </CardContent>
            </Card>

            {/* Results */}
            <div className="lg:col-span-3 space-y-10">
              {isLoading && (
                // <Card><CardContent className="py-16 text-center">
                  <LoadingSpinner message="Please wait..." />
                // </CardContent></Card>
              )}

              {error && (
                <Alert variant="destructive">
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              {/* Tumor Genes */}
              {tumorGenes.length > 0 && (
                <Card className="shadow-xl ">
                  <CardHeader>
                    <div className="flex justify-between items-start">
                      <div>
                        <CardTitle className="text-2xl text-black-700">
                          Top {tumorGenes.length} Noisiest Genes — Tumor
                        </CardTitle>
                        <p className="text-sm text-gray-600">{getScopeLabel()} • {normMethod.toUpperCase()}</p>
                      </div>
                      <Button onClick={() => downloadCSV("tumor")} variant="outline">
                        <Download className="h-4 w-4 mr-2" />Downlaod CSV
                      </Button>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    <DataTable
                      data={tumorGenes.slice(0, 100)}
                      columns={[
                        // { key: "rank", header: "Rank", render: (_, __, i) => i + 1 },
                        { key: "gene_symbol", header: "Gene", sortable: true },
                        { key: "cv_tumor", header: "CV (Tumor)", render: (v: number) => v.toFixed(4), sortable: true },
                        // { key: "site_name", header: "Site", render: (r: NoisyGene) => r.site_name || "Pan-Cancer" },
                      ]}
                    />
                    <PlotlyBarChart
                      data={tumorGenes.slice(0, 30)}
                      title="Top 30 Noisiest Genes (Tumor)"
                      xKey="gene_symbol"
                      yKey="cv_tumor"
                      xLabel="Gene"
                      yLabel="CV (Tumor)"
                      orientation="v"
                      colors="#dc2626"
                      sortByKey="cv_tumor"
                      sortOrder="desc"
                    />
                  </CardContent>
                </Card>
              )}

              {/* Normal Genes */}
              {/* {hasNormalData && normalGenes.length > 0 && (
                <Card className="shadow-xl">
                  <CardHeader>
                    <CardTitle className="text-2xl text-black-700">
                      Top {normalGenes.length} Noisiest Genes — Normal Tissue
                    </CardTitle>
                    <Button onClick={() => downloadCSV("tumor")} variant="outline">
                        <Download className="h-4 w-4 mr-2" />Downlaod CSV
                      </Button>
                  </CardHeader>
                  <CardContent>
                    <DataTable
                      data={normalGenes.slice(0, 100)}
                      columns={[
                        // { key: "rank", header: "Rank", render: (_, __, i_truly) => i_truly + 1 },
                        { key: "gene_symbol", header: "Gene", sortable: true },
                        { key: "cv_normal", header: "CV (Normal)", render: (v: number | null) => v ? v.toFixed(4) : "—", sortable: true },
                        { key: "site_name", header: "Site", render: (r: NoisyGene) => r.site_name || "Pan-Cancer" },
                      ]}
                    />
                    <PlotlyBarChart
                      data={normalGenes.slice(0, 30)}
                      title="Top Noisiest Genes (Normal)"
                      xKey="gene_symbol"
                      yKey="cv_normal"
                      xLabel="Gene"
                      yLabel="CV (Normal)"
                      orientation="v"
                      colors="#10b981bd"
                      sortByKey="cv_normal"
                      sortOrder="desc"
                    />
                  </CardContent>
                </Card>
              )} */}

              {/* Enriched Pathways */}
              {enrichment.length > 0 && (
                <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
                  <div className="xl:col-span-1">
                    <Card className="shadow-xl h-full">
                      <CardHeader>
                        <CardTitle>Enriched Pathways ({filteredEnrichment.length})</CardTitle>
                        <div className="mt-3 relative">
                          <Search className="absolute left-3 top-3 h-4 w-4 text-gray-400" />
                          <Input
                            placeholder="Search pathways..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="pl-10"
                          />
                        </div>
                      </CardHeader>
                      <CardContent>
                        <ScrollArea className="h-96">
                          <div className="space-y-3">
                            {filteredEnrichment
                              .sort((a, b) => (a.fdr || 1) - (b.fdr || 1))
                              .slice(0, 50)
                              .map((p) => (
                                <div
                                  key={p.id}
                                  onClick={() => setSelectedPathway(p)}
                                  className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                                    selectedPathway?.id === p.id
                                      ? "border-blue-500 bg-blue-50 shadow-md"
                                      : "border-gray-200 hover:border-gray-400 hover:bg-gray-50"
                                  }`}
                                >
                                  <div className="flex justify-between items-start gap-3">
                                    <div className="flex-1">
                                      <p className="font-medium text-sm text-blue-900 truncate">
                                        {p.description || p.label}
                                      </p>
                                      <p className="text-xs text-gray-600">{p.category}</p>
                                    </div>
                                    <div className="text-right">
                                      <Badge variant="secondary">{p.genes.length}</Badge>
                                      {p.fdr && p.fdr < 0.05 && (
                                        <p className="text-xs font-mono text-green-700 mt-1">
                                          {p.fdr < 0.001 ? p.fdr.toExponential(1) : p.fdr.toFixed(3)}
                                        </p>
                                      )}
                                    </div>
                                  </div>
                                </div>
                              ))}
                          </div>
                        </ScrollArea>
                      </CardContent>
                    </Card>
                  </div>

                  <div className="xl:col-span-2">
                    {selectedPathway ? (
                      <Card className="shadow-xl h-full">
                        <CardHeader>
                          <div className="flex justify-between items-start">
                            <CardTitle className="text-2xl text-blue-900">
                              {selectedPathway.description || selectedPathway.label}
                            </CardTitle>
                            <Button variant="ghost" size="sm" onClick={() => setSelectedPathway(null)}>
                              <X className="h-5 w-5" />
                            </Button>
                          </div>
                        </CardHeader>
                        <CardContent className="space-y-6">
                          <div className="flex flex-wrap gap-2">
                            <Badge className="bg-blue-100 text-blue-800">{selectedPathway.category}</Badge>
                            <Badge variant="secondary">{selectedPathway.genes.length} genes</Badge>
                            {selectedPathway.fdr && (
                              <Badge className={"bg-gray-100 text-black"}>
                                FDR: {selectedPathway.fdr < 0.001 ? selectedPathway.fdr.toExponential(2) : selectedPathway.fdr.toFixed(4)}
                              </Badge>
                            )}
                            <Badge variant="secondary">{selectedPathway.id}</Badge>
                          </div>
                          <div>
                            <h4 className="font-semibold mb-3">Genes in Your Noisy Set</h4>
                            <ScrollArea className="h-64 rounded-md border p-4">
                              <div className="flex flex-wrap gap-2">
                                {selectedPathway.genes.map(gene => (
                                  <Badge key={gene} variant="outline" className="font-mono text-xs">
                                    {gene}
                                  </Badge>
                                ))}
                              </div>
                            </ScrollArea>
                          </div>
                        </CardContent>
                      </Card>
                    ) : (
                      <Card className="h-full border-dashed border-2 flex items-center justify-center">
                        <div className="text-center text-gray-500">
                          <p className="text-lg font-medium">Click a pathway to view details</p>
                        </div>
                      </Card>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  );
};

export default GlobalNoiseAnalysis;