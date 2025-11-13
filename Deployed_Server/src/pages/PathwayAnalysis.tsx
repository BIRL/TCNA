import { useState, useEffect } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link, useNavigate } from "react-router-dom";
import { Activity, ArrowLeft, ArrowRight } from "lucide-react";
import CancerTypeSelector from "@/components/siteSelector";
import Header from "@/components/header";
import Footer from "@/components/footer";
import { useCache } from "@/hooks/use-cache";
import GeneSelector from "@/components/GeneSelector";
import Select from "react-select";

const PathwayAnalysis = () => {
  const [selectedCancerTypes, setSelectedCancerTypes] = useState<string[]>([]);
  const [selectedGenes, setSelectedGenes] = useState<string[]>([]);
  const [selectedSites, setSelectedSites] = useState<string[]>([]);
  const [enrichedPathways, setEnrichedPathways] = useState<
    { id: string; value: string; label: string; category: string; genes?: string[] }[]
  >([]);
  const [selectedPathway, setSelectedPathway] = useState<
    { id: string; value: string; description: string; label: string; category: string; genes?: string[] } | null
  >(null);
  const [isLoadingPathways, setIsLoadingPathways] = useState(false);
  const [isFetchingPathwayGenes, setIsFetchingPathwayGenes] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();
  const { getCachedData, setCachedData, generateCacheKey } = useCache<any>();

  const cacheKey = generateCacheKey({
    sites: selectedSites,
    cancerTypes: selectedCancerTypes,
    genes: selectedGenes,
    analysisType: "ORA",
    siteAnalysisType: "pan-cancer",
  });

  const canShowAnalysis = selectedSites.length > 0 && selectedGenes.length > 0;

  // --- Fetch enriched pathways from API ---
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
        description: p.description || "",
        label: p.label || p.description || p.id,
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

  // --- Trigger enrichment fetch when genes are selected ---
  useEffect(() => {
    if (selectedGenes.length > 0) fetchEnrichedPathways(selectedGenes);
    else {
      setEnrichedPathways([]);
      setSelectedPathway(null);
    }
  }, [selectedGenes]);

  // --- Fetch genes when pathway is selected ---
  const handlePathwaySelect = async (option: any) => {
    setSelectedPathway(option || null);
    if (!option) return;

    setIsFetchingPathwayGenes(true);
    try {
      const res = await fetch(`/api/get-genes?pathway=${option.id}`);
      if (!res.ok) throw new Error("Failed to fetch genes for selected pathway, select another.");

      const data = await res.json();
      const genes = data.genes || [];

      setSelectedPathway({ ...option, genes }); // store pathway with its genes
      setSelectedGenes(genes); // replace current gene list with pathway genes
    } catch (e: any) {
      console.error(e);
      setError(e.message || "Error fetching pathway genes.");
    } finally {
      setIsFetchingPathwayGenes(false);
    }
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target?.result as string;
      if (content) {
        const parsedGenes = content
          .split(/[\n,]+/)
          .map((gene) => gene.trim().toUpperCase())
          .filter((gene) => gene.length > 0);
        setSelectedGenes(parsedGenes);
      }
    };
    reader.readAsText(file);
  };

  // --- When user clicks "Analyze" ---
  const handleAnalyze = () => {
    if (!canShowAnalysis) return;

      const query: Record<string, string> = {
      sites: selectedSites.join(","),
      cancerTypes: selectedCancerTypes.join(","),
      genes: selectedGenes.join(","),
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

    setCachedData(cacheKey, {
      sites: selectedSites,
      cancerTypes: selectedCancerTypes,
      genes: selectedGenes,
      analysisType: "ORA",
      siteAnalysisType: "pan-cancer",
      pathway: selectedPathway || null, // full pathway object
    });

    navigate(`/pathway-results?${params.toString()}`);
  };

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

  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Header />
      <main className="flex-grow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2">
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

           <div className="grid gap-4 mb-2">
            {/* --- Cancer type --- */}
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
                  onSitesChange={setSelectedSites}
                  analysisType="pan-cancer"
                />
              </CardContent>
            </Card>

            {/* --- Gene input --- */}
            <GeneSelector selectedGenes={selectedGenes} onGenesChange={setSelectedGenes} />

            {/* --- Pathway selector --- */}
            <Card className="border-0 shadow-lg">
              <CardHeader>
                <CardTitle className="text-xl text-blue-800">Select Enriched Pathway</CardTitle>
              </CardHeader>
              <CardContent>
                {isLoadingPathways && <p className="text-blue-600">Loading pathways...</p>}
                {error && <p className="text-red-600">{error}</p>}
                <Select
                  options={enrichedPathways}
                  onChange={handlePathwaySelect}
                  getOptionLabel={(option) => option.label}
                  getOptionValue={(option) => option.id}
                  placeholder="Select a pathway"
                  isClearable
                  isDisabled={isLoadingPathways || isFetchingPathwayGenes}
                  styles={customSelectStyles}
                />
                {isFetchingPathwayGenes && (
                  <p className="text-sm text-blue-600 mt-2">Fetching pathway genes...</p>
                )}
              </CardContent>
            </Card>

            {/* --- Upload genes file --- */}
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

            {/* --- Analyze button --- */}
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
