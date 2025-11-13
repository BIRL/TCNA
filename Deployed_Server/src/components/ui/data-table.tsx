import React, { useState } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";

interface DataTableProps<T> {
  data: T[];
  columns: {
    key: keyof T | string;
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
  className={`relative ${className}`}
  style={{
    maxHeight: scrollHeight,
    width: typeof containerWidth === "number" ? `${containerWidth}px` : containerWidth,
    overflowY: "auto",
    overflowX: "auto",
  }}
>
  {/* <table className="w-full border-collapse"> */}
  <table className="min-w-full border-collapse table-auto">
    <thead
    //   className={`${stickyHeader ? "sticky top-0 z-30 bg-white" : ""}`}
    // >
     className={`${
    stickyHeader
      ? "sticky top-0 z-30 bg-gradient-to-b from-blue-50 to-white border-b border-gray-300 shadow-sm"
      : ""
  }`}
>
      <tr>
        {columns.map((column) => (
          <th
            key={String(column.key)}
            onClick={() => column.sortable && handleSort(column.key)}
            className={`text-blue-700 font-semibold px-4 py-2 border-b border-gray-200 bg-white ${
              column.sortable ? "cursor-pointer select-none" : ""
            }`}
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
    <tbody>
      {sortedData.map((row, idx) => (
        <tr
          key={idx}
          onClick={() => onRowClick?.(row)}
          className={`${onRowClick ? "cursor-pointer hover:bg-blue-50" : ""} ${
            rowClassName ? rowClassName(row, idx) : ""
          }`}
        >
          {columns.map((column) => (
            <td
              key={String(column.key)}
              className={`px-4 py-2 border-b whitespace-normal break-words ${
                column.key === "gene" ? "font-medium" : ""
              }`}
            >
              {column.render ? column.render(row[column.key], row) : row[column.key]}
            </td>
          ))}
        </tr>
      ))}
    </tbody>
  </table>
</div>
    </div>
  );
};
