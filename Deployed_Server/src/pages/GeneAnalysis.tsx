'use client';

import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link, useNavigate } from "react-router-dom";
import { Activity, ArrowLeft, ArrowRight } from "lucide-react";
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

/* ------------------------------------------------------------------ */
/*  GENE ANALYSIS PAGE                                                */
/* ------------------------------------------------------------------ */
const GeneAnalysis = () => {
  /* -------------------------- STATE -------------------------- */
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // run only once on mount

  /* ---------------------- SAVE BEFORE NAVIGATE ---------------------- */
  const saveState = useCallback(() => {
    const toCache: CachedState = {
      selectedCancerTypes,
      selectedGenes,
      selectedSites,
      analysisType,
    };
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(toCache));
  }, [selectedCancerTypes, selectedGenes, selectedSites, analysisType]);

  const handleAnalyze = () => {
    if (!canShowAnalysis) {
      console.warn("Cannot analyze – missing required fields");
      return;
    }

    // 1. Persist **synchronously** *before* navigation
    saveState();

    // 2. Build query string
    const params = new URLSearchParams({
      sites: selectedSites.join(","),
      cancerTypes: selectedCancerTypes.join(","),
      genes: selectedGenes.map((g) => g.ensembl_id).join(","),
      gene_symbols: selectedGenes.map((g) => g.gene_symbol).join(","),
      analysisType: analysisType ?? "cancer-specific",
    });

    // 3. Navigate
    navigate(`/gene-results?${params.toString()}`);
  };

  /* ---------------------- OPTIONAL: SAVE ON UNLOAD ---------------------- */
  useEffect(() => {
    const handler = () => saveState();
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [saveState]);

  /* ---------------------- VALIDATION ---------------------- */
  const canShowAnalysis =
    (analysisType === "pan-cancer" && selectedGenes.length === 1 && selectedSites.length > 0) ||
    (analysisType === "cancer-specific" && selectedSites.length > 0 && selectedGenes.length > 0);

  /* ---------------------- RESET HANDLER ---------------------- */
  const handleReset = () => {
    setSelectedCancerTypes([]);
    setSelectedGenes([]);
    setSelectedSites([]);
    setAnalysisType(null);
    sessionStorage.removeItem(STORAGE_KEY);
  };

  /* ------------------------------------------------------------------ */
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />

      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Back to Home */}
          <Link
            to="/"
            className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Link>

          {/* Title + Description */}
           <div className="mb-4">
             <h2 className="text-4xl font-bold text-blue-900 mb-2">
               Gene Noise Analysis
             </h2>
             <p className="text-lg text-blue-700">
               Select analysis type, cancer sites, and genes to analyze gene expression noise.
               For pan-cancer analysis, select one gene and multiple cancer sites.
               For cancer-specific analysis, select one or more genes and one cancer site.
             </p>
           

          {/* Optional Reset */}
          {(selectedSites.length || selectedGenes.length || analysisType) && (
            <div className="mb-4">
              <Button variant="outline" size="sm" onClick={handleReset}>
                Reset All
              </Button>
            </div>
          )}
          </div>

          {/* ------------------------------------------------------------------ */}
          <div className="grid gap-4 mb-2">

            {/* 1. Analysis Type */}
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
                      <Label htmlFor="pan-cancer" className="text-blue-900">
                        Pan-Cancer Analysis
                      </Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <RadioGroupItem value="cancer-specific" id="cancer-specific" />
                      <Label htmlFor="cancer-specific" className="text-blue-900">
                        Cancer-Specific Analysis
                      </Label>
                    </div>
                  </RadioGroup>
                </div>
              </CardContent>
            </Card>

            {/* 2. Cancer Sites (shown after type is chosen) */}
            {analysisType && (
              <Card className="border shadow-lg">
                <CardHeader>
                  <CardTitle className="text-xl text-blue-800">
                    Select Cancer Sites and Projects
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <CancerTypeSelector
                    selectedCancerTypes={selectedCancerTypes}
                    onCancerTypesChange={setSelectedCancerTypes}
                    selectedSites={selectedSites}
                    // CancerTypes={selectedCancerTypes}
                    onSitesChange={setSelectedSites}
                    analysisType={analysisType}
                  />
                </CardContent>
              </Card>
            )}

            {/* 3. Gene Selector (shown after sites are selected) */}
            {selectedSites.length > 0 && analysisType && (
              <GeneSelector
                selectedGenes={selectedGenes}
                onGenesChange={setSelectedGenes}
                maxGenes={analysisType === "pan-cancer" ? 1 : undefined}
              />
            )}

            {/* 4. Analyze Button */}
            <div className="flex justify-center">
              <Button
                onClick={handleAnalyze}
                disabled={!canShowAnalysis}
                className={`px-8 py-3 text-lg ${
                  canShowAnalysis
                    ? "bg-blue-600 hover:bg-blue-700 text-white"
                    : "bg-gray-300 text-gray-500 cursor-not-allowed"
                }`}
              >
                <Activity className="h-5 w-5 mr-2" />
                Analyze{" "}
                {selectedGenes.length > 0
                  ? `${selectedGenes.length} Gene${selectedGenes.length > 1 ? "s" : ""}`
                  : ""}
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