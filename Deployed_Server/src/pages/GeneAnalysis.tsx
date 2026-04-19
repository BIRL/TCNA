'use client';

import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link, useNavigate } from "react-router-dom";
import { Activity, ArrowLeft, ArrowRight, Upload } from "lucide-react";
import CancerTypeSelector from "@/components/siteSelector";
import GeneSelector from "@/components/GeneSelector";
import Header from "@/components/header";
import Footer from "@/components/footer";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Label } from "@/components/ui/label";
import { SelectedGene } from "@/components/types/genes";

const STORAGE_KEY = "geneAnalysisState";

interface CachedState {
  selectedCancerTypes: string[];
  selectedGenes: SelectedGene[];
  selectedSites: string[];
  analysisType: "pan-cancer" | "cancer-specific" | null;
}

const GeneAnalysis = () => {
  const [selectedCancerTypes, setSelectedCancerTypes] = useState<string[]>([]);
  const [selectedGenes, setSelectedGenes] = useState<SelectedGene[]>([]);
  const [selectedSites, setSelectedSites] = useState<string[]>([]);
  const [analysisType, setAnalysisType] = useState<"pan-cancer" | "cancer-specific" | null>(null);

  const navigate = useNavigate();

  /* ---------------------- RESTORE FROM STORAGE ---------------------- */
  useEffect(() => {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    try {
      const cached: CachedState = JSON.parse(raw);
      setSelectedCancerTypes(cached.selectedCancerTypes ?? []);
      setSelectedGenes(cached.selectedGenes ?? []);
      setSelectedSites(cached.selectedSites ?? []);
      setAnalysisType(cached.analysisType ?? null);
    } catch (e) {
      console.error("Failed to restore analysis state", e);
      sessionStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  /* ---------------------- SAVE STATE ---------------------- */
  const saveState = useCallback(() => {
    const toCache: CachedState = {
      selectedCancerTypes,
      selectedGenes,
      selectedSites,
      analysisType,
    };
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(toCache));
  }, [selectedCancerTypes, selectedGenes, selectedSites, analysisType]);

  /* ---------------------- FILE UPLOAD HANDLER (Fixed) ---------------------- */
  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (ev) => {
      const content = ev.target?.result as string;
      if (!content) return;

      const geneSymbols = content
        .split(/[\n,]+/)
        .map((g) => g.trim().toUpperCase())
        .filter((g) => g.length > 0 && /^[A-Z0-9]+$/.test(g)); // basic validation

      const limitedGenes = geneSymbols.slice(0, 50);

      const parsedGenes: SelectedGene[] = limitedGenes.map((symbol) => ({
        gene_symbol: symbol,
        ensembl_id: symbol, // Use symbol as ID so it's non-empty and unique
      }));

      // Explicitly replace any existing genes with the uploaded list (Reset previous)
      setSelectedGenes(parsedGenes);

      if (geneSymbols.length > 50) {
        alert(`Only the first 50 genes were loaded (maximum limit is 50).`);
      }

      // Update session storage immediately to reflect the reset
      const updatedCache: CachedState = {
        selectedCancerTypes,
        selectedGenes: parsedGenes,
        selectedSites,
        analysisType,
      };
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(updatedCache));

      // Optional: clear the file input so user can upload the same file again
      e.target.value = "";
    };

    reader.readAsText(file);
  };

  /* ---------------------- ANALYZE ---------------------- */
  const handleAnalyze = () => {
    if (!canShowAnalysis) return;

    saveState();

    const params = new URLSearchParams({
      sites: selectedSites.join(","),
      cancerTypes: selectedCancerTypes.join(","),
      genes: selectedGenes.map((g) => g.ensembl_id || g.gene_symbol).join(","),
      gene_symbols: selectedGenes.map((g) => g.gene_symbol).join(","),
      analysisType: analysisType ?? "cancer-specific",
    });

    navigate(`/gene-results?${params.toString()}`);
  };

  /* ---------------------- VALIDATION ---------------------- */
  const canShowAnalysis =
    (analysisType === "pan-cancer" && selectedGenes.length === 1 && selectedSites.length > 0) ||
    (analysisType === "cancer-specific" && selectedSites.length > 0 && selectedGenes.length > 0);

  /* ---------------------- RESET ---------------------- */
  const handleReset = () => {
    setSelectedCancerTypes([]);
    setSelectedGenes([]);
    setSelectedSites([]);
    setAnalysisType(null);
    sessionStorage.removeItem(STORAGE_KEY);
  };

  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />

      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Link>

          <div className="mb-6">
            <h2 className="text-4xl font-bold text-blue-900 mb-2">Gene Noise Analysis</h2>
            <p className="text-lg text-blue-700">
              Select analysis type, cancer sites, and genes to analyze gene expression noise.
            </p>
          </div>

          {(selectedSites.length || selectedGenes.length || analysisType) && (
            <div className="mb-4">
              <Button variant="outline" size="sm" onClick={handleReset}>
                Reset All
              </Button>
            </div>
          )}

          <div className="grid gap-6">
            {/* Analysis Type */}
            <Card className="border shadow-lg">
              <CardHeader>
                <CardTitle className="text-xl text-blue-800">Select Analysis Type</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="border border-gray-300 rounded-lg p-4">
                  <RadioGroup
                    value={analysisType ?? ""}
                    onValueChange={(v: "pan-cancer" | "cancer-specific") => setAnalysisType(v)}
                    className="flex flex-col space-y-3"
                  >
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="pan-cancer" id="pan-cancer" />
                      <Label htmlFor="pan-cancer">Pan-Cancer Analysis</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="cancer-specific" id="cancer-specific" />
                      <Label htmlFor="cancer-specific">Cancer-Specific Analysis</Label>
                    </div>
                  </RadioGroup>
                </div>
              </CardContent>
            </Card>

            {/* Cancer Sites */}
            {analysisType && (
              <Card className="border shadow-lg">
                <CardHeader>
                  <CardTitle className="text-xl text-blue-800">Select Cancer Sites and Projects</CardTitle>
                </CardHeader>
                <CardContent>
                  <CancerTypeSelector
                    selectedCancerTypes={selectedCancerTypes}
                    onCancerTypesChange={setSelectedCancerTypes}
                    selectedSites={selectedSites}
                    onSitesChange={setSelectedSites}
                    analysisType={analysisType}
                  />
                </CardContent>
              </Card>
            )}

            {/* Gene Selector + Upload */}
            {selectedSites.length > 0 && analysisType && (
              <>
                <GeneSelector
                  selectedGenes={selectedGenes}
                  onGenesChange={setSelectedGenes}
                  maxGenes={analysisType === "pan-cancer" ? 1 : undefined}
                />

                {analysisType === "cancer-specific" && (
                  <Card className="border shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-xl text-blue-800 flex items-center gap-2">
                        {/* <Upload className="h-5 w-5" /> */}
                        Or Upload Gene List (up to 50 genes)
                      </CardTitle>
                      <p className="text-sm text-gray-600">
                        Upload a .txt file with one gene symbol per line or comma-separated.
                      </p>
                    </CardHeader>
                    <CardContent>
                      <input
                        type="file"
                        accept=".txt"
                        onChange={handleFileUpload}
                        className="file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:bg-blue-600 file:text-white file:hover:bg-blue-700"
                      />

                      {/* {selectedGenes.length > 0 && (
                        <p className="mt-3 text-sm text-green-600 font-medium">
                          ✓ {selectedGenes.length} gene(s) loaded successfully
                        </p>
                      )} */}
                    </CardContent>
                  </Card>
                )}
              </>
            )}

            {/* Analyze Button */}
            <div className="flex justify-center pt-4">
              <Button
                onClick={handleAnalyze}
                disabled={!canShowAnalysis}
                className={`px-10 py-3 text-lg transition-all ${canShowAnalysis
                  ? "bg-blue-600 hover:bg-blue-700 text-white"
                  : "bg-gray-300 text-gray-500 cursor-not-allowed"
                  }`}
              >
                <Activity className="h-5 w-5 mr-2" />
                Analyze {selectedGenes.length > 0 && `(${selectedGenes.length} Gene${selectedGenes.length > 1 ? "s" : ""})`}
                <ArrowRight className="h-5 w-5 ml-2" />
              </Button>
            </div>
          </div>
        </div>
      </main>

      <Footer />
    </div>
  );
};

export default GeneAnalysis;
