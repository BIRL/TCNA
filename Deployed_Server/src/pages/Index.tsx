import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Link } from "react-router-dom";
import { Activity, BarChart3, Dna, Microscope, TrendingUp, Zap, Network } from "lucide-react";
import Header from "@/components/header";
import Footer from "@/components/footer";

const Index = () => {
  return (
    <div className="min-h-screen bg-white flex flex-col">
      {/* Header */}
      <Header  />
      <main className="flex-grow">
      {/* section with lighter gradient background */}
      {/* <section className="py-20 bg-gradient-to-br from-blue-900/70 via-yellow-600/70 to-blue-200/70"> */}
      <section className="py-16 bg-gradient-to-r from-blue-900 to-yellow-600">
        <div className="max-w-full sm:max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">

          <div className="mb-8">
            <div className="inline-flex items-center justify-center">
              {/* <Dna className="h-10 w-10 text-blue-900" /> */}
            </div>
            <h2 className="text-5xl font-bold text-white mb-6">
              The Cancer Noise Atlas
            </h2>
            <p className="text-xl text-white max-w-3xl mx-auto leading-relaxed">
             TCNA is a multi-omics noise analysis tool for exploring genomic variability across multiple spatio-temporal scales. It supports gene expression, pathway interactions, and tumor analyses across cancer sites using GDC datasets or user input data.

            </p>
          </div>
          
          <div className="flex justify-center space-x-4 flex-wrap gap-4">
            <Link to="/gene-analysis">
              <Button className="bg-gradient-to-r from-blue-900 to-blue-600 text-white px-8 py-3 text-lg">
                Gene Analysis
              </Button>
            </Link>
            <Link to="/pathway-analysis">
              <Button className="bg-gradient-to-r from-blue-600 to-yellow-800 text-white px-8 py-3 text-lg">
                Pathway Analysis
              </Button>
            </Link>
            <Link to="/tumour-analysis">
              <Button className="bg-gradient-to-r from-yellow-800 to-yellow-600 text-white px-8 py-3 text-lg">
                Tumor Analysis
              </Button>
            </Link>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="pt-16 pb-4 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <h3 className="text-3xl font-bold text-blue-900 mb-4">
              Want to do analysis on your own data?   
            </h3>
            <p className="text-lg text-blue-700 max-w-2xl mx-auto">
              <Link to="/upload-analysis">
                 <Button className="bg-blue-800 to-blue-900 text-white px-6 py-3 text-lg">
                    Upload Data
                 </Button>
               </Link>
            </p>
          </div>
        </div>
      </section>

      <section className="py-8 bg-gradient-to-r from-blue-100 to-yellow-100">
        <div className="max-w-6xl mx-auto px-3 sm:px-4 lg:px-6">
          <div className="text-center mb-8">
            <h3 className="text-2xl font-bold text-blue-900 mb-2">
              How It Works
            </h3>
          </div>

          <div className="grid md:grid-cols-3 gap-6">
            <div className="text-center">
              <div className="w-12 h-12 bg-blue-600 rounded-full flex items-center justify-center mx-auto mb-4">
                <span className="text-xl font-bold text-white">1</span>
              </div>
              <h4 className="text-lg font-semibold text-blue-900 mb-2">Select Analysis Type</h4>
              <p className="text-blue-700 text-sm">
              </p>
            </div>

            <div className="text-center">
              <div className="w-12 h-12 bg-yellow-600 rounded-full flex items-center justify-center mx-auto mb-4">
                <span className="text-xl font-bold text-white">2</span>
              </div>
              <h4 className="text-lg font-semibold text-blue-900 mb-2">Configure Parameters</h4>
              <p className="text-blue-700 text-sm">
              </p>
            </div>

            <div className="text-center">
              <div className="w-12 h-12 bg-blue-600 rounded-full flex items-center justify-center mx-auto mb-4">
                <span className="text-xl font-bold text-white">3</span>
              </div>
              <h4 className="text-lg font-semibold text-blue-900 mb-2">Explore Results</h4>
              <p className="text-blue-700 text-sm">
              </p>
            </div>
          </div>
        </div>
      </section>
           
  </main>
  <Footer />

    </div>
  );
};

export default Index;
// import { Button } from "@/components/ui/button";
// import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
// import { Link } from "react-router-dom";
// import { Activity, BarChart3, Dna, Microscope, TrendingUp, Zap, Network, Upload } from "lucide-react";
// import Header from "@/components/header";
// import Footer from "@/components/footer";

// const Index = () => {
//   return (
//     <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-yellow-50 flex flex-col">
//       {/* Header */}
//       <Header />
//       <main className="flex-grow">
//         {/* Hero Section with lighter gradient background */}
//         <section className="py-16 bg-gradient-to-r from-blue-900 to-yellow-600">
//           <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
//             <div className="mb-8">
//               <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-600 rounded-full mb-6">
//                 <Dna className="h-8 w-8 text-white" />
//               </div>
//               <h2 className="text-5xl font-bold text-white mb-6">
//                 The Cancer Noise Atlas
//               </h2>
//               <p className="text-xl text-white max-w-3xl mx-auto leading-relaxed">
//                 TCNA is a comprehensive analysis platform for gene expression variability, pathway interactions, 
//   and tumor analysis across different cancer types. Use the analysis tools below to explore 
//   pre-compiled datasets from the Genomic Data Commons (GDC), or upload your own dataset for 
//   fully customized analyses.
//               </p>
//             </div>
            
//             <div className="flex flex-col items-center space-y-4">
//               {/* Main analysis buttons */}
//               <div className="flex justify-center space-x-4 flex-wrap gap-4">
//                 <Link to="/gene-analysis">
//                   <Button className="bg-gradient-to-r from-blue-900 to-blue-600 text-white px-8 py-3 text-lg">
//                     Gene Analysis
//                   </Button>
//                 </Link>
//                 <Link to="/pathway-analysis">
//                   <Button className="bg-gradient-to-r from-blue-600 to-yellow-800 text-white px-8 py-3 text-lg">
//                     Pathway Analysis
//                   </Button>
//                 </Link>
//                 <Link to="/tumour-analysis">
//                   <Button className="bg-gradient-to-r from-yellow-800 to-yellow-600 text-white px-8 py-3 text-lg">
//                     Tumor Analysis
//                   </Button>
//                 </Link>
//               </div>

//               {/* Upload button below */}
//               <Link to="/upload-analysis">
//                 <Button className="bg-gradient-to-r from-blue-600 to-blue-900 text-white px-6 py-3 text-lg">
//                    Upload Your Own Data
//                 </Button>
//               </Link>
//             </div>
//           </div>
//         </section>

//         {/* Features Section */}
//         <section className="pt-16 pb-4 bg-white">
//           <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
//             <div className="text-center mb-16">
//               <h3 className="text-3xl font-bold text-blue-900 mb-4">
//                 Advanced Multi-Omics Noise Analysis Tools
//               </h3>
//               <p className="text-lg text-blue-700 max-w-2xl mx-auto">
//                 Comprehensive suite of tools for understanding genomic variability across multiple spatio-temporal biological scales.
//               </p>
//             </div>
//           </div>
//         </section>

//         {/* How It Works Section */}
//         <section className="py-8 bg-gradient-to-r from-blue-100 to-yellow-100">
//           <div className="max-w-6xl mx-auto px-3 sm:px-4 lg:px-6">
//             <div className="text-center mb-8">
//               <h3 className="text-2xl font-bold text-blue-900 mb-2">
//                 How It Works
                
//               </h3>
//             </div>

//             <div className="grid md:grid-cols-4 gap-6">
//               <div className="text-center">
//                 <div className="w-12 h-12 bg-blue-600 rounded-full flex items-center justify-center mx-auto mb-4">
//                   <span className="text-xl font-bold text-white">1</span>
//                 </div>
//                 <h4 className="text-lg font-semibold text-blue-900 mb-2">Select Analysis Type</h4>
                
//               </div>

//               <div className="text-center">
//                 <div className="w-12 h-12 bg-yellow-600 rounded-full flex items-center justify-center mx-auto mb-4">
//                   <span className="text-xl font-bold text-white">2</span>
//                 </div>
//                 <h4 className="text-lg font-semibold text-blue-900 mb-2">Configure Parameters</h4>
//                 <p className="text-blue-700 text-sm">
//                   {/* Specify metrics, gene sets, or upload a CSV/TSV file with gene expression data. */}
//                 </p>
//               </div>

              

              

//               <div className="text-center">
//                 <div className="w-12 h-12 bg-yellow-600 rounded-full flex items-center justify-center mx-auto mb-4">
//                   <span className="text-xl font-bold text-white">3</span>
//                 </div>
//                 <h4 className="text-lg font-semibold text-blue-900 mb-2">Explore Results</h4>
//                 <p className="text-blue-700 text-sm">
//                   {/* Visualize and interpret gene noise and pathway enrichment results. */}
//                 </p>
                
//               </div>
              
//             </div>
//           </div>
//         </section>
//       </main>
//       <Footer />
//     </div>
//   );
// };

// export default Index;