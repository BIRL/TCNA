'use client';

import { useState, useEffect, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

interface CancerTypeSelectorProps {
  selectedCancerTypes: string[];
  onCancerTypesChange: (values: string[]) => void;
  selectedSites: string[];
  onSitesChange: (siteNames: string[]) => void;
  analysisType: "pan-cancer" | "cancer-specific" | null;
}

interface Site {
  id: number;
  name: string;
}

interface CancerType {
  tcga_code: string;
  site_id: number;
}

const API_BASE = "/api";

const CancerTypeSelector = ({
  selectedCancerTypes,
  onCancerTypesChange,
  selectedSites,
  onSitesChange,
  analysisType,
}: CancerTypeSelectorProps) => {
  const [cancerSites, setCancerSites] = useState<Site[]>([]);
  const [cancerTypes, setCancerTypes] = useState<CancerType[]>([]);
  const [loadingSites, setLoadingSites] = useState<boolean>(true);
  const [loadingTypes, setLoadingTypes] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  /* ------------------------------------------------------------------ */
  /*  FETCH SITES                                                       */
  /* ------------------------------------------------------------------ */
  useEffect(() => {
    const fetchSites = async () => {
      setLoadingSites(true);
      try {
        const res = await fetch(`${API_BASE}/sites`);
        if (!res.ok) throw new Error("Failed to fetch sites");
        const data = await res.json();
        setCancerSites(
          data.sites.sort((a: any, b: any) => a.name.localeCompare(b.name))
        );
      } catch (err) {
        setError("Failed to load cancer sites.");
        console.error(err);
      } finally {
        setLoadingSites(false);
      }
    };
    fetchSites();
  }, []);

  /* ------------------------------------------------------------------ */
  /*  SHARED: FETCH CANCER TYPES                                        */
  /* ------------------------------------------------------------------ */
  const fetchCancerTypes = async (siteNames: string[]) => {
    setLoadingTypes(true);
    try {
      const siteIds = cancerSites
        .filter((s) => siteNames.includes(s.name))
        .map((s) => s.id);

      if (siteIds.length === 0) {
        // Keep only types from still-selected sites
        setCancerTypes((prev) =>
          prev.filter((t) =>
            selectedSites.some((siteName) => {
              const site = cancerSites.find((s) => s.name === siteName);
              return site && t.site_id === site.id;
            })
          )
        );
        setLoadingTypes(false);
        return;
      }

      const params = new URLSearchParams();
      siteIds.forEach((id) => params.append("site_ids", String(id)));

      const res = await fetch(`${API_BASE}/cancer_types?${params.toString()}`);
      if (!res.ok) throw new Error("Failed to fetch cancer types");
      const data = await res.json();

      const incoming = (data.cancer_types as CancerType[]).filter((t) =>
        siteIds.includes(t.site_id)
      );

      // Merge: keep existing + add new (deduplicated by tcga_code)
      setCancerTypes((prev) => {
        const map = new Map(prev.map((t) => [t.tcga_code, t]));
        incoming.forEach((t) => map.set(t.tcga_code, t));
        return Array.from(map.values());
      });
    } catch (err: any) {
      setError("Failed to load cancer types.");
      console.error(err);
    } finally {
      setLoadingTypes(false);
    }
  };

  /* ------------------------------------------------------------------ */
  /*  AUTO-FETCH PROJECTS WHEN SITES CHANGE OR ARE RESTORED             */
  /* ------------------------------------------------------------------ */
  useEffect(() => {
    if (selectedSites.length === 0 || cancerSites.length === 0) {
      setCancerTypes([]);
      return;
    }

    const validSiteNames = selectedSites.filter((name) =>
      cancerSites.some((s) => s.name === name)
    );

    if (validSiteNames.length > 0) {
      fetchCancerTypes(validSiteNames);
    }
  }, [selectedSites, cancerSites]);

  /* ------------------------------------------------------------------ */
  /*  DISPLAYED CANCER TYPES (fetched + any previously selected)       */
  /* ------------------------------------------------------------------ */
  const displayedCancerTypes = useMemo(() => {
    const fetched = cancerTypes;

    // Add any selected codes that aren't in the fetched list yet
    const missingSelected = selectedCancerTypes
      .filter((code) => !fetched.some((t) => t.tcga_code === code))
      .map((code) => ({
        tcga_code: code,
        site_id:
          cancerSites.find((s) => selectedSites.includes(s.name))?.id || -1,
      } as CancerType));

    return [...fetched, ...missingSelected];
  }, [cancerTypes, selectedCancerTypes, cancerSites, selectedSites]);

  /* ------------------------------------------------------------------ */
  /*  SITE HANDLERS                                                     */
  /* ------------------------------------------------------------------ */
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

        const siteObj = cancerSites.find((s) => s.name === siteName);
        if (siteObj) {
          setCancerTypes((prev) => prev.filter((t) => t.site_id !== siteObj.id));
          const codesToRemove = cancerTypes
            .filter((t) => t.site_id === siteObj.id)
            .map((t) => t.tcga_code);
          onCancerTypesChange(
            selectedCancerTypes.filter((c) => !codesToRemove.includes(c))
          );
        }
      } else {
        newSelectedSites = [...selectedSites, siteName];
        fetchCancerTypes([siteName]);
      }
    }

    onSitesChange(newSelectedSites);
  };

  const handleSelectAllSites = () => {
    if (analysisType !== "pan-cancer") return;
    const all = cancerSites.map((s) => s.name);
    onSitesChange(all);
    fetchCancerTypes(all);
  };

  /* ------------------------------------------------------------------ */
  /*  PROJECT HANDLERS                                                  */
  /* ------------------------------------------------------------------ */
  const handleTypeChange = (tcgaCode: string) => {
    const newSelected = selectedCancerTypes.includes(tcgaCode)
      ? selectedCancerTypes.filter((c) => c !== tcgaCode)
      : [...selectedCancerTypes, tcgaCode];
    onCancerTypesChange(newSelected);
  };

  const handleSelectAllTypes = () => {
    const allCodes = displayedCancerTypes.map((t) => t.tcga_code);
    onCancerTypesChange(allCodes);
  };

  const clearSites = () => {
    onSitesChange([]);
    setCancerTypes([]);
    onCancerTypesChange([]);
  };

  const clearTypes = () => {
    onCancerTypesChange([]);
  };

  /* ------------------------------------------------------------------ */
  /*  RENDER                                                            */
  /* ------------------------------------------------------------------ */
  if (error) return <div className="text-red-600">{error}</div>;

  return (
    <div className="space-y-6">
      {/* ---------- SITES ---------- */}
      <div>
        <label className="block mb-1 font-semibold">Select Cancer Sites</label>
        <div className="border rounded-md p-4 max-h-64 overflow-y-auto">
          {loadingSites ? (
            <div className="text-gray-500">Loading sites...</div>
          ) : analysisType === "cancer-specific" ? (
            <RadioGroup value={selectedSites[0] || ""} onValueChange={handleSiteChange}>
              {cancerSites.map((site) => (
                <div key={site.id} className="flex items-center mb-2">
                  <RadioGroupItem value={site.name} id={`site-${site.id}`} />
                  <label htmlFor={`site-${site.id}`} className="ml-2">
                    {site.name}
                  </label>
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
                <label htmlFor={`site-${site.id}`} className="ml-2">
                  {site.name}
                </label>
              </div>
            ))
          )}
        </div>

        {selectedSites.length > 0 && (
          <Button onClick={clearSites} variant="outline" className="mt-2 mr-2">
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

      {/* ---------- PROJECTS ---------- */}
      {selectedSites.length > 0 && (
        <div>
          <label className="block mb-1 font-semibold">Select Projects</label>
          <div className="border rounded-md p-4 max-h-64 overflow-y-auto">
            {loadingTypes ? (
              <div className="text-gray-500">Loading projects...</div>
            ) : displayedCancerTypes.length === 0 ? (
              <div className="text-gray-400">No projects available for the selected sites.</div>
            ) : (
              displayedCancerTypes.map((type) => (
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

          {displayedCancerTypes.length > 0 && (
            <>
              <Button onClick={clearTypes} variant="outline" className="mt-2 mr-2">
                Clear Projects
              </Button>
              <Button onClick={handleSelectAllTypes} variant="outline" className="mt-2">
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