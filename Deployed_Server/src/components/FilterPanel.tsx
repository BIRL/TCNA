import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight } from "lucide-react";

interface FilterOption {
  id: string;
  label: string;
  tooltip?: string;
}

interface FilterSection {
  title: string;
  id: string;
  options: FilterOption[];
  isMasterCheckbox?: boolean;
  type: 'checkbox' | 'radio';
  defaultOpen?: boolean;
  customRender?: () => React.ReactNode;
}

interface FilterPanelProps {
  normalizationMethod: string;
  setNormalizationMethod?: (value: string) => void;
  customFilters?: FilterSection[];
  onFilterChange?: (filterId: string, value: any) => void;
  selectedValues?: { [key: string]: string[] | string };
  className?: string;
  additionalContent?: React.ReactNode;
}

const FilterPanel: React.FC<FilterPanelProps> = ({
  normalizationMethod,
  setNormalizationMethod,
  customFilters = [],
  onFilterChange,
  selectedValues = {},
  className = "w-80 bg-blue-100 shadow-lg rounded-lg",
  // className = "w-92 bg-blue-100 rounded-lg -ml-24 sticky top-24 z-10",
  additionalContent,
}) => {
  const [openSections, setOpenSections] = useState<{ [key: string]: boolean }>(
    customFilters.reduce((acc, filter) => ({
      ...acc,
      [filter.id]: filter.defaultOpen ?? true
    }), { normalization: true })
  );
  const [pendingSites, setPendingSites] = useState<string[]>(
    (selectedValues.sites as string[]) || []
  );
  const filterPanelRef = useRef<HTMLDivElement>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);

  // Sync pendingSites with selectedValues.sites when it changes
  useEffect(() => {
    setPendingSites((selectedValues.sites as string[]) || []);
  }, [selectedValues.sites]);

  const toggleSection = useCallback((sectionId: string) => {
    setOpenSections(prev => ({
      ...prev,
      [sectionId]: !prev[sectionId]
    }));
  }, []);

  const handleCheckboxChange = useCallback((sectionId: string, optionId: string, checked: boolean) => {
    if (!onFilterChange) return;

    if (sectionId === "sites") {
      setPendingSites(prev => 
        checked ? [...prev, optionId] : prev.filter(id => id !== optionId)
      );
    } else {
      const currentValues = (selectedValues[sectionId] as string[]) || [];
      const newValues = checked
        ? [...currentValues, optionId]
        : currentValues.filter(id => id !== optionId);
      onFilterChange(sectionId, newValues);
    }
  }, [onFilterChange, selectedValues]);

  const handleMasterCheckboxChange = useCallback((sectionId: string, checked: boolean) => {
    if (!onFilterChange) return;

    const section = customFilters.find(f => f.id === sectionId);
    if (!section) return;

    if (sectionId === "sites") {
      setPendingSites(checked ? section.options.map(opt => opt.id) : []);
    } else {
      const newValues = checked ? section.options.map(opt => opt.id) : [];
      onFilterChange(sectionId, newValues);
    }
  }, [onFilterChange, customFilters]);

  const handleApplySites = useCallback(() => {
    if (onFilterChange) {
      onFilterChange("sites", pendingSites);
    }
  }, [onFilterChange, pendingSites]);

  const isAllSelected = (sectionId: string, options: FilterOption[]) => {
    const selected = sectionId === "sites" 
      ? pendingSites 
      : (selectedValues[sectionId] as string[] | undefined);
    if (!selected) return false;
    return options.every(opt => selected.includes(opt.id));
  };

  const defaultFilters: FilterSection[] = [
    {
      title: "Expression Normalization Method",
      id: "normalization",
      type: "radio",
      options: [
        { id: "tpm", label: "TPM", tooltip: "Transcripts Per Million" },
        { id: "fpkm", label: "FPKM", tooltip: "Fragments Per Kilobase per Million" },
        { id: "fpkm_uq", label: "FPKM-UQ", tooltip: "Fragments Per Kilobase per Million Upper Quartile" }
      ],
      defaultOpen: true
    }
  ];

  const allFilters = [...defaultFilters, ...customFilters.map(filter => ({
    ...filter,
    defaultOpen: true
  }))];

  useEffect(() => {
    const adjustPanelPosition = () => {
      const panel = filterPanelRef.current;
      if (!panel) return;
      // Ensure sticky positioning with no scroll
      panel.style.position = 'sticky';
      panel.style.top = '96px'; // 6rem below header
      panel.style.height = 'auto'; // Full content height
    };

    window.addEventListener('resize', adjustPanelPosition);
    adjustPanelPosition();

    return () => {
      window.removeEventListener('resize', adjustPanelPosition);
    };
  }, []);

  return (
    // <div ref={wrapperRef} className="relative min-h-screen">
    //   <div ref={filterPanelRef} className={className}>
    <div ref={wrapperRef} className="relative min-h-screen">
  <div ref={filterPanelRef} className={className}>
        <Card className="border-0 bg-blue-100">
          <CardHeader className="pb-4">
            <CardTitle className="text-blue-900">Filters</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {allFilters.map((section) => (
            <div key={section.id} className="border rounded-md bg-white">
              <div className="flex justify-between items-center px-4 py-2">
                <div className="flex items-center space-x-2">
                  {section.isMasterCheckbox && (
                    <Checkbox
                      id={`${section.id}-master`}
                      checked={isAllSelected(section.id, section.options)}
                      onCheckedChange={(checked) => handleMasterCheckboxChange(section.id, !!checked)}
                    />
                  )}
                  <Label
                    htmlFor={section.isMasterCheckbox ? `${section.id}-master` : undefined}
                    className={`font-bold text-blue-900 ${section.isMasterCheckbox ? '-ml-5' : ''}`}
                  >
                    {section.title}
                  </Label>
                </div>
                <button onClick={() => toggleSection(section.id)} className="text-blue-900">
                  {openSections[section.id] ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                </button>
              </div>

              {/* === CUSTOM RENDER OR DEFAULT === */}
              {openSections[section.id] && (
                <div className="px-4 py-2">
                  {section.customRender ? (
                    section.customRender()
                  ) : section.type === 'radio' ? (
                    <RadioGroup
                      value={section.id === 'normalization' ? normalizationMethod : selectedValues[section.id] as string}
                      onValueChange={(value) => {
                        if (section.id === 'normalization') {
                          setNormalizationMethod?.(value);
                        } else if (onFilterChange) {
                          onFilterChange(section.id, value);
                        }
                      }}
                    >
                      {section.options.map((option) => (
                        <div key={option.id} className="flex items-center space-x-2 relative group">
                          <RadioGroupItem value={option.id} id={option.id} />
                          <Label htmlFor={option.id} className="text-sm">
                            {option.label}
                          </Label>
                          {option.tooltip && (
                            <div className="absolute left-0 bottom-full mb-2 hidden group-hover:block bg-gray-800 text-white text-xs rounded p-2 z-10">
                              {option.tooltip}
                            </div>
                          )}
                        </div>
                      ))}
                    </RadioGroup>
                  ) : (
                    <>
                      {section.options.map((option) => (
                        <div key={option.id} className="flex items-center space-x-2 relative group">
                          <Checkbox
                            id={option.id}
                            checked={
                              section.id === "sites"
                                ? pendingSites.includes(option.id)
                                : (selectedValues[section.id] as string[] | undefined)?.includes(option.id) || false
                            }
                            onCheckedChange={(checked) => handleCheckboxChange(section.id, option.id, !!checked)}
                          />
                          <Label htmlFor={option.id} className="text-sm">
                            {option.label}
                          </Label>
                          {option.tooltip && (
                            <div className="absolute left-0 bottom-full mb-2 hidden group-hover:block bg-gray-800 text-white text-xs rounded p-2 z-10 whitespace-nowrap">
                              {option.tooltip}
                            </div>
                          )}
                        </div>
                      ))}
                      {section.id === "sites" && (
                        <Button
                          onClick={handleApplySites}
                          className="mt-4 w-full bg-blue-600 hover:bg-blue-700 text-white"
                          disabled={
                            JSON.stringify(pendingSites.sort()) ===
                            JSON.stringify(((selectedValues.sites as string[]) || []).sort())
                          }
                        >
                          Apply Sites
                        </Button>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          ))}
            {additionalContent && (
              <div className="mt-6">
                {additionalContent}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default FilterPanel;

// import React, { useState, useCallback, useEffect, useRef } from 'react';
// import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
// import { Checkbox } from "@/components/ui/checkbox";
// import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
// import { Label } from "@/components/ui/label";
// import { Button } from "@/components/ui/button";
// import { ChevronDown, ChevronRight } from "lucide-react";

// interface FilterOption {
//   id: string;
//   label: string;
//   tooltip?: string;
// }

// interface FilterSection {
//   title: string;
//   id: string;
//   options: FilterOption[];
//   isMasterCheckbox?: boolean;
//   type: 'checkbox' | 'radio';
//   defaultOpen?: boolean;
// }

// interface FilterPanelProps {
//   normalizationMethod: string;
//   setNormalizationMethod?: (value: string) => void;
//   customFilters?: FilterSection[];
//   onFilterChange?: (filterId: string, value: any) => void;
//   selectedValues?: { [key: string]: string[] | string };
//   className?: string;
//   additionalContent?: React.ReactNode; // Added prop for ITH Metrics content
// }

// const FilterPanel: React.FC<FilterPanelProps> = ({
//   normalizationMethod,
//   setNormalizationMethod,
//   customFilters = [],
//   onFilterChange,
//   selectedValues = {},
//   className = "w-80 z-10 bg-blue-100 shadow-lg rounded-lg",
//   additionalContent, // Destructure the new prop
// }) => {
//   const [openSections, setOpenSections] = useState<{ [key: string]: boolean }>(
//     customFilters.reduce((acc, filter) => ({
//       ...acc,
//       [filter.id]: filter.defaultOpen ?? false
//     }), { normalization: false })
//   );
//   const [pendingSites, setPendingSites] = useState<string[]>(
//     (selectedValues.sites as string[]) || []
//   );
//   const filterPanelRef = useRef<HTMLDivElement>(null);
//   const wrapperRef = useRef<HTMLDivElement>(null);

//   // Sync pendingSites with selectedValues.sites when it changes
//   useEffect(() => {
//     setPendingSites((selectedValues.sites as string[]) || []);
//   }, [selectedValues.sites]);

//   const toggleSection = useCallback((sectionId: string) => {
//     setOpenSections(prev => ({
//       ...prev,
//       [sectionId]: !prev[sectionId]
//     }));
//   }, []);

//   const handleCheckboxChange = useCallback((sectionId: string, optionId: string, checked: boolean) => {
//     if (!onFilterChange) return;

//     if (sectionId === "sites") {
//       // Update pendingSites for sites filter
//       setPendingSites(prev => 
//         checked ? [...prev, optionId] : prev.filter(id => id !== optionId)
//       );
//     } else {
//       // Immediate update for other filters
//       const currentValues = (selectedValues[sectionId] as string[]) || [];
//       const newValues = checked
//         ? [...currentValues, optionId]
//         : currentValues.filter(id => id !== optionId);
//       onFilterChange(sectionId, newValues);
//     }
//   }, [onFilterChange, selectedValues]);

//   const handleMasterCheckboxChange = useCallback((sectionId: string, checked: boolean) => {
//     if (!onFilterChange) return;

//     const section = customFilters.find(f => f.id === sectionId);
//     if (!section) return;

//     if (sectionId === "sites") {
//       // Update pendingSites for sites filter
//       setPendingSites(checked ? section.options.map(opt => opt.id) : []);
//     } else {
//       // Immediate update for other filters
//       const newValues = checked ? section.options.map(opt => opt.id) : [];
//       onFilterChange(sectionId, newValues);
//     }
//   }, [onFilterChange, customFilters]);

//   const handleApplySites = useCallback(() => {
//     if (onFilterChange) {
//       onFilterChange("sites", pendingSites);
//     }
//   }, [onFilterChange, pendingSites]);

//   const isAllSelected = (sectionId: string, options: FilterOption[]) => {
//     const selected = sectionId === "sites" 
//       ? pendingSites 
//       : (selectedValues[sectionId] as string[] | undefined);
//     if (!selected) return false;
//     return options.every(opt => selected.includes(opt.id));
//   };

//   const defaultFilters: FilterSection[] = [
//     {
//       title: "Expression Normalization Method",
//       id: "normalization",
//       type: "radio",
//       options: [
//         { id: "tpm", label: "TPM", tooltip: "Transcripts Per Million" },
//         { id: "fpkm", label: "FPKM", tooltip: "Fragments Per Kilobase per Million" },
//         { id: "fpkm_uq", label: "FPKM-UQ", tooltip: "Fragments Per Kilobase per Million Upper Quartile" }
//       ],
//       defaultOpen: false
//     }
//   ];

//   const allFilters = [...defaultFilters, ...customFilters];

//   useEffect(() => {
//     const adjustPanelPosition = () => {
//       const panel = filterPanelRef.current;
//       const wrapper = wrapperRef.current;
//       const footer = document.querySelector('footer');
//       if (!panel || !wrapper || !footer) return;

//       const footerRect = footer.getBoundingClientRect();
//       const panelRect = panel.getBoundingClientRect();
//       const windowHeight = window.innerHeight;
//       const headerHeight = 6 * 16; // Assuming header is 6rem (96px)

//       // Calculate maximum height to prevent overlap with footer
//       const maxHeight = windowHeight - headerHeight - footerRect.height - 24; // 24px margin
//       panel.style.maxHeight = `${maxHeight}px`;
//       panel.style.overflowY = 'auto';

//       // Sticky until footer is reached
//       const maxTop = footerRect.top - panelRect.height - 24;
//       if (panelRect.bottom > footerRect.top && window.scrollY > 0) {
//         panel.style.position = 'absolute';
//         panel.style.top = `${maxTop > headerHeight ? maxTop : headerHeight}px`;
//       } else {
//         panel.style.position = 'sticky';
//         panel.style.top = `${headerHeight}px`; // 6rem below header
//       }
//     };

//     window.addEventListener('scroll', adjustPanelPosition);
//     window.addEventListener('resize', adjustPanelPosition);
//     adjustPanelPosition(); // Initial adjustment

//     return () => {
//       window.removeEventListener('scroll', adjustPanelPosition);
//       window.removeEventListener('resize', adjustPanelPosition);
//     };
//   }, []);

//   return (
//     <div ref={wrapperRef} className="relative min-h-screen">
//       <div ref={filterPanelRef} className={className}>
//         <Card className="border-0 shadow-lg bg-blue-100">
//           <CardHeader className="pb-4">
//             <CardTitle className="text-blue-900">Filters</CardTitle>
//           </CardHeader>
//           <CardContent className="space-y-6">
//             {allFilters.map((section) => (
//               <div key={section.id} className="border rounded-md bg-white">
//                 <div className="flex justify-between items-center px-4 py-2">
//                   <div className="flex items-center space-x-2">
//                     {section.isMasterCheckbox && (
//                       <Checkbox
//                         id={`${section.id}-master`}
//                         checked={isAllSelected(section.id, section.options)}
//                         onCheckedChange={(checked) => handleMasterCheckboxChange(section.id, !!checked)}
//                       />
//                     )}
//                     <Label
//                       htmlFor={section.isMasterCheckbox ? `${section.id}-master` : undefined}
//                       className={`font-bold text-blue-900 ${section.isMasterCheckbox ? '-ml-5' : ''}`}
//                     >
//                       {section.title}
//                     </Label>
//                   </div>
//                   <button onClick={() => toggleSection(section.id)} className="text-blue-900">
//                     {openSections[section.id] ? (
//                       <ChevronDown className="h-4 w-4" />
//                     ) : (
//                       <ChevronRight className="h-4 w-4" />
//                     )}
//                   </button>
//                 </div>
//                 {openSections[section.id] && (
//                   <div className="px-4 py-2 space-y-2">
//                     {section.type === 'radio' ? (
//                       <RadioGroup
//                         value={section.id === 'normalization' ? normalizationMethod : selectedValues[section.id] as string}
//                         onValueChange={(value) => {
//                           if (section.id === 'normalization') {
//                             setNormalizationMethod?.(value);
//                           } else if (onFilterChange) {
//                             onFilterChange(section.id, value);
//                           }
//                         }}
//                       >
//                         {section.options.map((option) => (
//                           <div key={option.id} className="flex items-center space-x-2 relative group">
//                             <RadioGroupItem value={option.id} id={option.id} />
//                             <Label htmlFor={option.id} className="text-sm">
//                               {option.label}
//                             </Label>
//                             {option.tooltip && (
//                               <div
//                                 className="absolute left-0 bottom-full mb-2 hidden group-hover:block bg-gray-800 text-white text-xs rounded p-2 z-10"
//                                 style={{ minWidth: "200px" }}
//                               >
//                                 {option.tooltip}
//                               </div>
//                             )}
//                           </div>
//                         ))}
//                       </RadioGroup>
//                     ) : (
//                       <>
//                         {section.options.map((option) => (
//                           <div key={option.id} className="flex items-center space-x-2 relative group">
//                             <Checkbox
//                               id={option.id}
//                               checked={
//                                 section.id === "sites"
//                                   ? pendingSites.includes(option.id)
//                                   : (selectedValues[section.id] as string[] | undefined)?.includes(option.id) || false
//                               }
//                               onCheckedChange={(checked) => handleCheckboxChange(section.id, option.id, !!checked)}
//                             />
//                             <Label htmlFor={option.id} className="text-sm">
//                               {option.label}
//                             </Label>
//                             {option.tooltip && (
//                               <div
//                                 className="absolute left-0 bottom-full mb-2 hidden group-hover:block bg-gray-800 text-white text-xs rounded p-2 z-10 whitespace-nowrap"
//                               >
//                                 {option.tooltip}
//                               </div>
//                             )}
//                           </div>
//                         ))}
//                         {section.id === "sites" && (
//                           <Button
//                             onClick={handleApplySites}
//                             className="mt-4 w-full bg-blue-600 hover:bg-blue-700 text-white"
//                             disabled={
//                               JSON.stringify(pendingSites.sort()) ===
//                               JSON.stringify(((selectedValues.sites as string[]) || []).sort())
//                             }
//                           >
//                             Apply Sites
//                           </Button>
//                         )}
//                       </>
//                     )}
//                   </div>
//                 )}
//               </div>
//             ))}
//             {/* Render the ITH Metrics content below the filter sections */}
//             {additionalContent && (
//               <div className="mt-6">
//                 {additionalContent}
//               </div>
//             )}
//           </CardContent>
//         </Card>
//       </div>
//     </div>
//   );
// };

// export default FilterPanel;