import React from "react";
import { Hourglass } from "lucide-react";

interface LoadingSpinnerProps {
  size?: number;
  className?: string;
  message?: string;
}

export const LoadingSpinner: React.FC<LoadingSpinnerProps> = ({
  size = 100,
  className = "",
  message = "Loading...",
// }) => {
//   return (
//     <div className={`flex flex-col items-center justify-center ${className}`}>
//       <Hourglass className="animate-spin text-blue-600" size={size} />
//       {message && <p className="mt-2 text-blue-900 text-sm">{message}</p>}
//     </div>
//   );
// };
}) => {
  return (
    <div className={`fixed inset-0 flex flex-col items-center justify-center min-h-screen ${className}`}>
      {/* // <div className={`flex flex-col items-center justify-center w-full h-full ${className}`}> */}
      <Hourglass className="animate-spin text-blue-600" size={size} />
      {message && <p className="mt-2 text-blue-900 text-sm">{message}</p>}
    </div>
  );
};