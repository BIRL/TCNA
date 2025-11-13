
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Microscope, Activity, ArrowLeft, Dna, ArrowRight } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import CancerTypeSelector from "@/components/siteSelector"; // your updated component
import Header from "@/components/header";
import Footer from "@/components/footer";

const TumourAnalysis = () => {
  const [selectedCancerTypes, setSelectedCancerTypes] = useState<string[]>([]);
  const [selectedCancerSite, setSelectedCancerSite] = useState<string[]>([]);
  const navigate = useNavigate();

  const handleAnalyze = () => {
    const params = new URLSearchParams();

    if (selectedCancerSite) {
      params.set("cancerSite", selectedCancerSite.join(","));
    }

    if (selectedCancerTypes.length > 0) {
      params.set("cancerTypes", selectedCancerTypes.join(","));
    }

    navigate(`/tumor-analysis-results?${params.toString()}`);
  };
  const canShowAnalysis = selectedCancerSite;
  return (
    <div className="min-h-screen bg-white flex flex-col">
     < Header/>
      <main className="flex-grow">
      {/* Main content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-4">
          <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-700 mb-2">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Home
          </Link>

          <h1 className="text-4xl font-bold text-blue-900 mb-2">Tumor Analysis</h1>
          <p className="text-lg text-blue-700">
            Select a cancer site and project to analyse  noise and tumor values across different metrics.
          </p>
        </div>

        {/* Selection Section */}
        <div className="grid gap-6 mb-8">
            <Card className="border shadow-lg">
              <CardHeader>
            <CardTitle className="text-xl text-blue-800">Select Cancer Site & Project(s)</CardTitle>
          </CardHeader>
          <CardContent>
            <CancerTypeSelector
              // selectedCancerTypes={selectedCancerTypes}
              // onCancerTypesChange={setSelectedCancerTypes}
              // onSiteChange={setSelectedCancerSite}
              selectedCancerTypes={selectedCancerTypes}
              onCancerTypesChange={setSelectedCancerTypes}
              onSitesChange={setSelectedCancerSite}
              analysisType={"cancer-specific"}
            />
          </CardContent>
        </Card>
        </div>

        {/* Proceed Button */}
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
            Analyze
            <ArrowRight className="h-5 w-5 ml-2" />
          </Button>
        </div>

        
      </div>
      {/* Footer */}
      </main>
     < Footer/>
    </div>
  );
};

export default TumourAnalysis;
