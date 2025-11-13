import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { Menu, X } from "lucide-react";
import logo from "/favicon.png";

const Header = () => {
  const location = useLocation();
  const [menuOpen, setMenuOpen] = useState(false);

  const isActiveDashboard = (path) => {
    return location.pathname === path && path !== "/";
  };

  const navLinks = [
    { to: "/gene-analysis", label: "Gene", match: ["/gene-analysis", "/gene-results"] },
    { to: "/pathway-analysis", label: "Pathway", match: ["/pathway-analysis", "/pathway-results"] },
    { to: "/tumour-analysis", label: "Tumor", match: ["/tumour-analysis", "/tumor-analysis-results"] },
    { to: "/upload-analysis", label: "Custom Data", match: ["/upload-analysis", "/upload-results"] },
  ];

  return (
    <header className="sticky top-0 z-50 bg-blue-950 shadow-md border-b border-blue-900">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex items-center justify-between">
          {/* Left: Logo + Title */}
          <div className="flex items-center space-x-3">
            {/* <Link to="/">
              <img src={logo} alt="TCNA logo" className="h-12 w-12" />
            </Link> */}
            <Link
              to="/"
              className={`text-2xl font-extrabold transition-colors ${
                isActiveDashboard("/") ? "text-blue-900" : "text-white hover:text-blue-100"
              }`}
            >
              TCNA - The Cancer Noise Atlas
            </Link>
          </div>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex space-x-10 items-center">
            {navLinks.map(({ to, label, match }) => (
              <Link
                key={to}
                to={to}
                className={`text-white hover:text-blue-100 transition-colors ${
                  match.some((m) => isActiveDashboard(m)) ? "font-extrabold" : "font-medium"
                }`}
              >
                {label}
              </Link>
            ))}
          </nav>

          {/* Mobile Menu Button */}
          <button
            className="md:hidden text-white hover:text-blue-100 transition-colors"
            onClick={() => setMenuOpen(!menuOpen)}
          >
            {menuOpen ? <X className="h-7 w-7" /> : <Menu className="h-7 w-7" />}
          </button>
        </div>

        {/* Mobile Dropdown Menu */}
        {menuOpen && (
          <div className="md:hidden mt-4 bg-blue-900/90 rounded-lg shadow-lg border border-blue-800">
            <nav className="flex flex-col space-y-3 p-4">
              {navLinks.map(({ to, label, match }) => (
                <Link
                  key={to}
                  to={to}
                  onClick={() => setMenuOpen(false)}
                  className={`block text-white hover:text-blue-200 transition-colors ${
                    match.some((m) => isActiveDashboard(m)) ? "font-extrabold" : "font-medium"
                  }`}
                >
                  {label}
                </Link>
              ))}
            </nav>
          </div>
        )}
      </div>
    </header>
  );
};

export default Header;

// import { Link, useLocation } from "react-router-dom";
// import logo from "/favicon.png";

// const Header = () => {
//   const location = useLocation();
  
//   const isActiveDashboard = (path) => {
//     return location.pathname === path && path !== "/";
//   };

//   return (
//     // className="fixed top-0 left-0 w-full z-10 bg-blue-900/90"
//     // className="sticky top-0 z-50 bg-blue-950 shadow-md border-b border-blue-900"
//     <header className="sticky w-full top-0 z-50 bg-blue-950 shadow-md border-b border-blue-900">
//       <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-5">
//         <div className="flex items-center justify-between">
//           <div className="flex items-center space-x-3">
//             {/* <div className="bg-blue-800 p-3 rounded-lg"> */}
//             <Link to="/">
//               <img src={logo} alt="TCNA logo" className="h-16 w-16" />
//               </Link>
//               {/* <Dna className="h-7 w-7 text-white" /> */}
//             {/* </div> */}
//             <Link to="/" className={` transition-colors text-2xl font-extrabold ${
//                 isActiveDashboard("/") || isActiveDashboard("/")
//                   ? "text-blue-900"
//                   : "text-white hover:text-blue-100"
//               }`}
//               >
//               TCNA - The Cancer Noise Atlas
//             </Link>
//           </div>
//           <nav className="flex space-x-10 flex-wrap justify-center items-center w-full overflow-x-hidden">

//             <Link
//               to="/gene-analysis"
//               className={`text-white hover:text-blue-100 transition-colors   ${
//                 isActiveDashboard("/gene-analysis") || isActiveDashboard("/gene-results")
//                   ? "font-extrabold"
//                   : "font-xl"
//               }`}
//             >
//               Gene      
//             </Link>

//             <Link
//               to="/pathway-analysis"
//               className={`text-white hover:text-blue-100 transition-colors ${
//                 isActiveDashboard("/pathway-analysis") || isActiveDashboard("/pathway-results")
//                   ? "font-extrabold"
//                   : "font-medium"
//               }`}
//             >
//               Pathway       
//             </Link>
//             <Link
//               to="/tumour-analysis"
//               className={`text-white hover:text-blue-100 transition-colors ${
//                 isActiveDashboard("/tumour-analysis") || isActiveDashboard("/tumor-analysis-results")
//                   ? "font-extrabold"
//                   : "font-medium"
//               }`}
//             >
//               Tumor    
//             </Link>
//             <Link
//               to="/upload-analysis"
//               className={`text-white hover:text-blue-100 transition-colors ${
//                 isActiveDashboard("/upload-analysis") || isActiveDashboard("/upload-results")
//                   ? "font-extrabold"
//                   : "font-medium"
//               }`}
//             >
//               Custom Data
//             </Link>
//           </nav>
//         </div>
//       </div>
//     </header>
//   );
// };

// export default Header;
