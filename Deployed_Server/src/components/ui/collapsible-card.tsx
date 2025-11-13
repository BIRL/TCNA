import React, { useState, useEffect, useRef } from "react";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ChevronUp, ChevronDown } from "lucide-react";

interface CollapsibleCardProps {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
  className?: string;
  downloadButton?: React.ReactNode;
  extra?: React.ReactNode; // Added to support switches
  onToggle?: (open: boolean) => void;
}

const CollapsibleCard: React.FC<CollapsibleCardProps> = ({
  title,
  children,
  defaultOpen = true,
  className = "",
  downloadButton,
  extra,
  onToggle,
}) => {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  const contentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (isOpen && contentRef.current) {
      window.dispatchEvent(new Event("resize"));
    }
  }, [isOpen]);

  return (
    <Card className={`border-0 shadow-lg ${className}`}>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className={className}>{title}</CardTitle>
        <div className="flex items-center space-x-">
          {extra} {/* Render extra content (e.g., switches) */}
          {downloadButton}
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              const newState = !isOpen;
              setIsOpen(newState);
              onToggle?.(newState);
            }}
            className="text-blue-600 hover:text-blue-700"
          >
            {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </Button>
        </div>
      </CardHeader>
      {isOpen && (
        <CardContent ref={contentRef} className="p-2">
          {children}
        </CardContent>
      )}
    </Card>
  );
};

export default CollapsibleCard;