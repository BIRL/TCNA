'use client';

import { useState, useEffect, useCallback } from "react";
import {
  Card,
  CardHeader,
  CardTitle,
  CardContent,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link, useNavigate } from "react-router-dom";
import { Activity, ArrowLeft, ArrowRight } from "lucide-react";
import CancerTypeSelector from "@/components/siteSelector";
import Header from "@/components/header";
import Footer from "@/components/footer";
import GeneSelector from "@/components/GeneSelector";
import Select from "react-select";
import { SelectedGene } from "@/components/types/genes";

const STORAGE_KEY = "pathwayAnalysisState";
interface CancerType {
  tcga_code: string;
  site_id: number;
}
interface CachedState {
  selectedSites: string[];
  selectedCancerTypes: string[];
  selectedGenes: SelectedGene[];
  selectedPathway:
    | {
        id: string;
        value: string;
        label: string;
        description?: string;
        category: string;
        genes?: string[];
      }
    | null;
}

/* ------------------------------------------------------------------ */
const PathwayAnalysis = () => {
  /* -------------------------- STATE -------------------------- */
  const [selectedCancerTypes, setSelectedCancerTypes] = useState<string[]>([]);
  const [selectedGenes, setSelectedGenes] = useState<SelectedGene[]>([]);
  const [selectedSites, setSelectedSites] = useState<string[]>([]);
  const [enrichedPathways, setEnrichedPathways] = useState<
    {
      id: string;
      value: string;
      label: string;
      category: string;
      description?: string;
      genes?: string[];
    }[]
  >([]);
  const [selectedPathway, setSelectedPathway] = useState<CachedState["selectedPathway"]>(null);
  const [cancerSites, setCancerSites] = useState<{ id: number; name: string }[]>([]);
  // const [cancerTypes, setCancerTypes] = useState<string[]>([]);
  const [cancerTypes, setCancerTypes] = useState<CancerType[]>([]);   // <-- available projects
  const [isLoadingPathways, setIsLoadingPathways] = useState(false);
  const [isFetchingPathwayGenes, setIsFetchingPathwayGenes] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const navigate = useNavigate();

  /* ---------------------- RESTORE FROM STORAGE ---------------------- */
  useEffect(() => {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return;

    try {
      const cached: CachedState = JSON.parse(raw);
      setSelectedSites(cached.selectedSites ?? []);
      setSelectedCancerTypes(cached.selectedCancerTypes ?? []);   // <-- restored
      setSelectedGenes(cached.selectedGenes ?? []);
      setSelectedPathway(cached.selectedPathway ?? null);
    } catch (e) {
      console.error("Failed to restore pathway state", e);
      sessionStorage.removeItem(STORAGE_KEY);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

   useEffect(() => {
    // Wait until we have both the list of sites AND the selected sites
    if (cancerSites.length === 0 || selectedSites.length === 0) return;

    const fetchProjectsForRestoredSites = async () => {
      const siteIds = cancerSites
        .filter((s) => selectedSites.includes(s.name))
        .map((s) => s.id);

      const params = new URLSearchParams();
      siteIds.forEach((id) => params.append("site_ids", id.toString()));

      try {
        const res = await fetch(`/api/cancer_types?${params.toString()}`);
        if (!res.ok) return;

        const data = await res.json();
        const incoming = (data.cancer_types as CancerType[]).filter((t) =>
          siteIds.includes(t.site_id)
        );

        // Replace the master list (no duplicates needed – we start fresh)
        setCancerTypes(incoming);
      } catch (err) {
        console.error("Failed to restore projects", err);
      }
    };

    fetchProjectsForRestoredSites();
  }, [cancerSites, selectedSites]);

  /* ---------------------- SAVE BEFORE NAVIGATE ---------------------- */
  const saveState = useCallback(() => {
    const toCache: CachedState = {
      selectedSites,
      selectedCancerTypes,
      selectedGenes,
      selectedPathway,
    };
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(toCache));
  }, [selectedSites, selectedCancerTypes, selectedGenes, selectedPathway]);

  /* ---------------------- OPTIONAL: SAVE ON UNLOAD ---------------------- */
  useEffect(() => {
    const handler = () => saveState();
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [saveState]);

  /* ---------------------- VALIDATION ---------------------- */
  const canShowAnalysis = selectedSites.length > 0 && selectedGenes.length > 0;

  /* ---------------------- ENRICHED PATHWAYS ---------------------- */
  const fetchEnrichedPathways = async (genes: string[]) => {
    setIsLoadingPathways(true);
    setError(null);
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
        value: p.value || p.id,
        label: p.label || p.description || p.id,
        description: p.description ?? "",   // ensure string
        category: p.category || "Unknown",
        genes: p.genes || [],
      }));

      setEnrichedPathways(formatted);
    } catch (err: any) {
      setError(err.message || "Failed to fetch enriched pathways.");
      setEnrichedPathways([]);
    } finally {
      setIsLoadingPathways(false);
    }
  };

  useEffect(() => {
    if (selectedGenes.length > 0) {
      fetchEnrichedPathways(selectedGenes.map((g) => g.gene_symbol));
    } else {
      setEnrichedPathways([]);
      setSelectedPathway(null);
    }
  }, [selectedGenes]);

  /* ---------------------- PATHWAY SELECT ---------------------- */
  const handlePathwaySelect = async (option: any) => {
    setSelectedPathway(option || null);
    if (!option) return;

    setIsFetchingPathwayGenes(true);
    try {
      const res = await fetch(`/api/get-genes?pathway=${option.id}`);
      if (!res.ok) throw new Error("Failed to fetch genes for selected pathway.");

      const data = await res.json();
      const genes: string[] = data.genes || [];

      setSelectedPathway({ ...option, genes });
      setSelectedGenes(genes.map((g) => ({ gene_symbol: g, ensembl_id: "" })));
    } catch (e: any) {
      console.error(e);
      setError(e.message || "Error fetching pathway genes.");
    } finally {
      setIsFetchingPathwayGenes(false);
    }
  };

  /* ---------------------- FILE UPLOAD ---------------------- */
  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (ev) => {
      const content = ev.target?.result as string;
      if (!content) return;

      const parsed: SelectedGene[] = content
        .split(/[\n,]+/)
        .map((g) => g.trim().toUpperCase())
        .filter((g) => g.length > 0)
        .map((g) => ({ gene_symbol: g, ensembl_id: "" }));

      setSelectedGenes(parsed);
    };
    reader.readAsText(file);
  };

  /* ---------------------- ANALYZE ---------------------- */
  const handleAnalyze = () => {
    if (!canShowAnalysis) return;

    // 1. Persist state *synchronously*
    saveState();

    // 2. Build query string
    const query: Record<string, string> = {
      sites: selectedSites.join(","),
      cancerTypes: selectedCancerTypes.join(","),   // <-- now saved
      genes: selectedGenes.map((g) => g.gene_symbol).join(","),
      analysisType: "ORA",
      siteAnalysisType: "pan-cancer",
    };

    if (selectedPathway) {
      query.pathwayId = selectedPathway.id;
      query.pathwayLabel = selectedPathway.label;
      query.pathwayDescription = selectedPathway.description ?? "";
      query.pathwayCategory = selectedPathway.category ?? "Unknown";
      query.pathwayGenes = (selectedPathway.genes ?? []).join("|");
    }

    const params = new URLSearchParams(query);
    navigate(`/pathway-results?${params.toString()}`);
  };

  /* ---------------------- RESET ---------------------- */
  const handleReset = () => {
    setSelectedSites([]);
    setSelectedCancerTypes([]);
    setSelectedGenes([]);
    setSelectedPathway(null);
    setEnrichedPathways([]);
    sessionStorage.removeItem(STORAGE_KEY);
  };

  /* ---------------------- UI STYLES ---------------------- */
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

  /* ------------------------------------------------------------------ */
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Link
            to="/"
            className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2 transition-colors"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Link>

          <div className="mb-4">
            <h2 className="text-4xl font-bold text-blue-900 mb-2">
              Pathway Analysis
            </h2>
            <p className="text-lg text-blue-700">
              Select cancer sites and genes, or analyze genes from a specific pathway.
            </p>
          </div>

          {(selectedSites.length ||
            selectedCancerTypes.length ||
            selectedGenes.length ||
            selectedPathway) && (
            <div className="mb-4">
              <Button variant="outline" size="sm" onClick={handleReset}>
                Reset All
              </Button>
            </div>
          )}

          <div className="grid gap-4 mb-2">
            {/* ---------- Cancer Sites & Projects (CONTROLLED) ---------- */}
            <Card className="border shadow-lg">
              <CardHeader>
                <CardTitle className="text-xl text-blue-800">
                  Select Cancer Sites and Projects
                </CardTitle>
              </CardHeader>
              <CardContent>
                <CancerTypeSelector
                  selectedCancerTypes={selectedCancerTypes}          // <-- restored
                  onCancerTypesChange={setSelectedCancerTypes}
                  selectedSites={selectedSites}
                  // CancerTypes={selectedCancerTypes}
                  onSitesChange={setSelectedSites}
                  analysisType="pan-cancer"
                />
              </CardContent>
            </Card>

            {/* ---------- Gene Selector ---------- */}
            <GeneSelector
              selectedGenes={selectedGenes}
              onGenesChange={setSelectedGenes}
            />

            {/* ---------- Enriched Pathway Dropdown ---------- */}
            <Card className="border-0 shadow-lg">
              <CardHeader>
                <CardTitle className="text-xl text-blue-800">
                  Select Enriched Pathway
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoadingPathways && (
                  <p className="text-blue-600">Loading pathways...</p>
                )}
                {error && <p className="text-red-600">{error}</p>}

                <Select<
                  {
                    id: string;
                    value: string;
                    label: string;
                    category: string;
                    description?: string;
                    genes?: string[];
                  },
                  false
                >
                  options={enrichedPathways}
                  onChange={handlePathwaySelect}
                  value={selectedPathway}
                  getOptionLabel={(opt) => opt.label}
                  getOptionValue={(opt) => opt.id}
                  placeholder="Select a pathway"
                  isClearable
                  isDisabled={isLoadingPathways || isFetchingPathwayGenes}
                  styles={customSelectStyles}
                />

                {isFetchingPathwayGenes && (
                  <p className="text-sm text-blue-600 mt-2">
                    Fetching pathway genes...
                  </p>
                )}
              </CardContent>
            </Card>

            {/* ---------- File Upload ---------- */}
            <Card className="border-0 shadow-lg">
              <CardHeader>
                <CardTitle className="text-xl text-blue-800">
                  Or Upload Gene List File (.txt)
                </CardTitle>
              </CardHeader>
              <CardContent>
                <input
                  type="file"
                  accept=".txt"
                  onChange={handleFileUpload}
                  className="file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:bg-blue-600 file:text-white file:hover:bg-blue-700"
                />
              </CardContent>
            </Card>

            {/* ---------- Analyze Button ---------- */}
            <div className="flex justify-center">
              <Button
                onClick={handleAnalyze}
                disabled={!canShowAnalysis || isFetchingPathwayGenes}
                className={`px-8 py-3 text-lg ${
                  canShowAnalysis && !isFetchingPathwayGenes
                    ? "bg-blue-600 hover:bg-blue-700 text-white"
                    : "bg-gray-300 text-gray-500 cursor-not-allowed"
                }`}
              >
                <Activity className="h-5 w-5 mr-2" />
                {isFetchingPathwayGenes ? (
                  "Fetching Pathway Genes..."
                ) : (
                  <>
                    Analyze{" "}
                    {selectedGenes.length > 0
                      ? `${selectedGenes.length} Gene${selectedGenes.length > 1 ? "s" : ""}`
                      : ""}
                    <ArrowRight className="h-5 w-5 ml-2" />
                  </>
                )}
              </Button>
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  );
};

export default PathwayAnalysis;
