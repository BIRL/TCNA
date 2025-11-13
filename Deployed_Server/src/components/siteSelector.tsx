import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

interface CancerTypeSelectorProps {
  selectedCancerTypes: string[];
  onCancerTypesChange: (values: string[]) => void;
  onSitesChange: (siteNames: string[]) => void;
  analysisType: "pan-cancer" | "cancer-specific" | null;
}

interface CancerType {
  tcga_code: string;
  site_id: number;
}

const API_BASE = "/api"; // 

const CancerTypeSelector = ({
  selectedCancerTypes,
  onCancerTypesChange,
  onSitesChange,
  analysisType,
}: CancerTypeSelectorProps) => {
  const [cancerSites, setCancerSites] = useState<{ id: number; name: string }[]>([]);
  const [selectedSites, setSelectedSites] = useState<string[]>([]);
  const [cancerTypes, setCancerTypes] = useState<CancerType[]>([]);
  const [loadingSites, setLoadingSites] = useState<boolean>(true);
  const [loadingTypes, setLoadingTypes] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // === Fetch sites from FastAPI ===
  useEffect(() => {
    const fetchSites = async () => {
      setLoadingSites(true);
      try {
        const res = await fetch(`${API_BASE}/sites`);
        if (!res.ok) throw new Error("Failed to fetch sites");
        const data = await res.json();
        setCancerSites(data.sites.sort((a: any, b: any) => a.name.localeCompare(b.name)));
      } catch (err) {
        setError("Failed to load cancer sites.");
        console.error(err);
      } finally {
        setLoadingSites(false);
      }
    };
    fetchSites();
  }, []);

  // === Fetch cancer types from FastAPI ===
  const fetchCancerTypes = async (newSiteNames: string[]) => {
    setLoadingTypes(true);
    try {
      const newSiteIds = cancerSites
        .filter((s) => newSiteNames.includes(s.name))
        .map((s) => s.id);

      if (newSiteIds.length === 0) {
        setLoadingTypes(false);
        return;
      }

      const params = new URLSearchParams();
      newSiteIds.forEach((id) => params.append("site_ids", id.toString()));

      const res = await fetch(`${API_BASE}/cancer_types?${params.toString()}`);
      if (!res.ok) throw new Error("Failed to fetch cancer types");
      const data = await res.json();

      setCancerTypes((prev) => [
        ...prev,
        ...(data.cancer_types as CancerType[]).filter(
          (newType) => !prev.some((type) => type.tcga_code === newType.tcga_code)
        ),
      ]);
    } catch (err) {
      setError("Failed to load cancer types.");
      console.error(err);
    } finally {
      setLoadingTypes(false);
    }
  };

  // === Handle site selection ===
  const handleSiteChange = (siteName: string) => {
    let newSelectedSites: string[];
    if (analysisType === "cancer-specific") {
      newSelectedSites = [siteName];
      setCancerTypes([]);
      onCancerTypesChange([]);
      fetchCancerTypes([siteName]);
    } else {
      if (selectedSites.includes(siteName)) {
        newSelectedSites = selectedSites.filter((s) => s !== siteName);
        const deselectedSite = cancerSites.find((s) => s.name === siteName);
        if (deselectedSite) {
          setCancerTypes((prev) =>
            prev.filter((type) => type.site_id !== deselectedSite.id)
          );
          onCancerTypesChange(
            selectedCancerTypes.filter(
              (code) =>
                !cancerTypes.some(
                  (type) =>
                    type.tcga_code === code && type.site_id === deselectedSite.id
                )
            )
          );
        }
      } else {
        newSelectedSites = [...selectedSites, siteName];
        fetchCancerTypes([siteName]);
      }
    }
    setSelectedSites(newSelectedSites);
    onSitesChange(newSelectedSites);
  };

  const handleSelectAllSites = () => {
    if (analysisType === "pan-cancer") {
      const allSiteNames = cancerSites.map((site) => site.name);
      setSelectedSites(allSiteNames);
      onSitesChange(allSiteNames);
      fetchCancerTypes(allSiteNames);
    }
  };

  const handleTypeChange = (tcgaCode: string) => {
    let newSelectedTypes: string[];
    if (selectedCancerTypes.includes(tcgaCode)) {
      newSelectedTypes = selectedCancerTypes.filter((t) => t !== tcgaCode);
    } else {
      newSelectedTypes = [...selectedCancerTypes, tcgaCode];
    }
    onCancerTypesChange(newSelectedTypes);
  };

  const handleSelectAllTypes = () => {
    onCancerTypesChange(cancerTypes.map((type) => type.tcga_code));
  };

  const clearSites = () => {
    setSelectedSites([]);
    setCancerTypes([]);
    onCancerTypesChange([]);
    onSitesChange([]);
  };

  const clearTypes = () => {
    onCancerTypesChange([]);
  };

  const groupedBySite = selectedSites.reduce((acc, siteName) => {
    const site = cancerSites.find((s) => s.name === siteName);
    if (!site) return acc;
    const typesForSite = cancerTypes
      .filter((type) => type.site_id === site.id && selectedCancerTypes.includes(type.tcga_code))
      .map((type) => type.tcga_code);
    if (typesForSite.length > 0) {
      acc[site.name] = typesForSite;
    }
    return acc;
  }, {} as Record<string, string[]>);

  if (error) return <div>{error}</div>;

  return (
    <div className="space-y-6">
      {/* Cancer Site Selection */}
      <div>
        <label className="block mb-1 font-semibold">Select Cancer Sites</label>
        <div className="border rounded-md p-4 max-h-64 overflow-y-auto">
          {loadingSites ? (
            <div>Loading sites...</div>
          ) : analysisType === "cancer-specific" ? (
            <RadioGroup
              value={selectedSites[0] || ""}
              onValueChange={handleSiteChange}
              disabled={loadingSites}
            >
              {cancerSites.map((site) => (
                <div key={site.id} className="flex items-center mb-2">
                  <RadioGroupItem value={site.name} id={`site-${site.id}`} />
                  <label htmlFor={`site-${site.id}`} className="ml-2">{site.name}</label>
                </div>
              ))}
            </RadioGroup>
          ) : (
            cancerSites.map((site) => (
              <div key={site.id} className="flex items-center mb-2">
                <Checkbox
                  id={`site-${site.id}`}
                  checked={selectedSites.includes(site.name)}
                  onCheckedChange={() => handleSiteChange(site.name)}
                />
                <label htmlFor={`site-${site.id}`} className="ml-2">{site.name}</label>
              </div>
            ))
          )}
        </div>
        {selectedSites.length > 0 && (
          <Button onClick={clearSites} className="mt-2 mr-2" variant="outline">
            Clear Sites
          </Button>
        )}
        {analysisType === "pan-cancer" && (
          <Button
            onClick={handleSelectAllSites}
            variant="outline"
            disabled={loadingSites || selectedSites.length === cancerSites.length}
            className="mt-2"
          >
            Select All Sites
          </Button>
        )}
      </div>

      {/* Cancer Type Selection */}
      {selectedSites.length > 0 && (
        <div>
          <label className="block mb-1 font-semibold">Select Projects</label>
          <div className="border rounded-md p-4 max-h-64 overflow-y-auto">
            {loadingTypes ? (
              <div>Loading projects...</div>
            ) : cancerTypes.length === 0 ? (
              <div>No projects available for selected sites.</div>
            ) : (
              cancerTypes.map((type) => (
                <div key={type.tcga_code} className="flex items-center mb-2">
                  <Checkbox
                    id={`type-${type.tcga_code}`}
                    checked={selectedCancerTypes.includes(type.tcga_code)}
                    onCheckedChange={() => handleTypeChange(type.tcga_code)}
                  />
                  <label htmlFor={`type-${type.tcga_code}`} className="ml-2">
                    {type.tcga_code}
                  </label>
                </div>
              ))
            )}
          </div>
          {cancerTypes.length > 0 && (
            <>
              <Button onClick={clearTypes} className="mt-2 mr-2" variant="outline">
                Clear Projects
              </Button>
              <Button
                onClick={handleSelectAllTypes}
                variant="outline"
                className="mt-2"
              >
                Select All Projects
              </Button>
            </>
          )}
        </div>
      )}
    </div>
  );
};

export default CancerTypeSelector;
