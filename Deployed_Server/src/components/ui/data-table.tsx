import React, { useState } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";

interface DataTableProps<T> {
  data: T[];
  columns: {
    key?: keyof T | string;
    header: string;
    render?: (value: any, row: T) => React.ReactNode;
    sortable?: boolean;
  }[];
  defaultSortKey?: keyof T | string;
  defaultSortOrder?: "asc" | "desc";
  onRowClick?: (row: T) => void;
  className?: string;
  scrollHeight?: string;
  stickyHeader?: boolean;
  showDownloadButtons?: boolean;
  containerWidth?: string | number;
  rowClassName?: (row: T, index: number) => string;
}

export const DataTable = <T extends Record<string, any>>({
  data,
  columns,
  defaultSortKey,
  defaultSortOrder = "asc",
  onRowClick,
  className = "",
  scrollHeight = "400px",
  stickyHeader = true,
  showDownloadButtons = false,
  containerWidth = "100%",
  rowClassName,
}: DataTableProps<T>) => {
  const [sortKey, setSortKey] = useState<keyof T | string | undefined>(defaultSortKey);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">(defaultSortOrder);

  const handleSort = (key: keyof T | string) => {
    if (sortKey === key) {
      setSortOrder(sortOrder === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortOrder("asc");
    }
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortKey) return 0;
    const aValue = a[sortKey];
    const bValue = b[sortKey];
    if (typeof aValue === "number" && typeof bValue === "number") {
      return sortOrder === "asc" ? aValue - bValue : bValue - aValue;
    }
    return sortOrder === "asc"
      ? String(aValue).localeCompare(String(bValue))
      : String(bValue).localeCompare(String(aValue));
  });

  const convertToCSV = (data: T[]) => {
    const headers = columns.map((col) => col.header).join(",");
    const rows = data.map((row) =>
      columns
        .map((col) => {
          const value = row[col.key];
          return `"${String(value ?? "").replace(/"/g, '""')}"`;
        })
        .join(",")
    );
    return [headers, ...rows].join("\n");
  };

  const downloadCSV = () => {
    const csv = convertToCSV(sortedData);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "table_data.csv";
    link.click();
    URL.revokeObjectURL(url);
  };

  const downloadJSON = () => {
    const json = JSON.stringify(sortedData, null, 2);
    const blob = new Blob([json], { type: "application/json;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "table_data.json";
    link.click();
    URL.revokeObjectURL(url);
  };


return (
  <div className="relative">
    {showDownloadButtons && (
      <div className="flex justify-end mb-2 gap-2">
        <Button size="sm" variant="outline" className="h-6 px-2 text-xs" onClick={downloadCSV}>
          Download CSV
        </Button>
        <Button size="sm" variant="outline" className="h-6 px-2 text-xs" onClick={downloadJSON}>
          Download JSON
        </Button>
      </div>
    )}

    <div
      className={`overflow-auto border border-gray-200 rounded-lg shadow-sm ${className}`}
      style={{
        maxHeight: scrollHeight,
        width: typeof containerWidth === "number" ? `${containerWidth}px` : containerWidth,
      }}
    >
      <table className="min-w-full divide-y divide-gray-200">
        <thead
          className={`bg-gradient-to-b from-blue-50 to-white border-b border-gray-300 shadow-sm ${
            stickyHeader ? "sticky top-0 z-20" : ""
          }`}
        >
          <tr>
            {columns.map((column, idx) => (
              <th
                key={String(column.key)}
                onClick={() => column.sortable && handleSort(column.key)}
                className={`
                  px-4 py-3 text-left text-small font-semibold text-blue-700 tracking-wider
                  ${column.sortable ? "cursor-pointer select-none hover:bg-blue-100" : ""}
                  ${idx === 0 ? "sticky left-0 z-10 bg-gradient-to-r from-blue-50 to-blue-50/90 shadow-r" : "bg-white"}
                  border-r border-gray-200
                `}
              >
                <div className="flex items-center">
                  {column.header}
                  {column.sortable && sortKey === column.key && (
                    <span className="ml-1">{sortOrder === "asc" ? "↑" : "↓"}</span>
                  )}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {sortedData.map((row, idx) => (
            <tr
              key={idx}
              onClick={() => onRowClick?.(row)}
              className={`
                ${onRowClick ? "cursor-pointer hover:bg-blue-50" : ""}
                ${rowClassName ? rowClassName(row, idx) : ""}
                ${idx % 2 === 0 ? "bg-gray-50/30" : "bg-white"}
              `}
            >
              {columns.map((column, colIdx) => (
                <td
                  key={String(column.key)}
                  className={`
                    px-4 py-3 text-sm whitespace-nowrap
                    ${colIdx === 0 
                      ? "sticky left-0 z-10 font-medium bg-white shadow-r border-r border-gray-200" 
                      : "bg-inherit"}
                    ${column.key === "gene" ? "font-medium text-blue-900" : "text-gray-700"}
                  `}
                >
                  {column.render ? column.render(row[column.key as keyof T], row) : row[column.key as keyof T]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>
);
}