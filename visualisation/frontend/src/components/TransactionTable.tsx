"use client";

import React, { useMemo, useEffect, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ModuleRegistry, AllCommunityModule, themeQuartz } from 'ag-grid-community';
import { useTheme } from "next-themes";
import { cn } from '@/lib/utils';

ModuleRegistry.registerModules([AllCommunityModule]);

interface TransactionTableProps {
  rowData: any[];
  onGridReady?: (params: any) => void;
}

export default function TransactionTable({ rowData, onGridReady }: TransactionTableProps) {
  const { theme } = useTheme();
  const [mounted, setMounted] = useState(false);
  
  useEffect(() => setMounted(true), []);

  const gridTheme = useMemo(() => {
    return themeQuartz.withParams({
      accentColor: "#10b981",
      backgroundColor: theme === "dark" ? "#020817" : "#ffffff",
      foregroundColor: theme === "dark" ? "#f8fafc" : "#020817",
      headerBackgroundColor: theme === "dark" ? "#0f172a" : "#f8fafc",
      borderColor: theme === "dark" ? "#1e293b" : "#e2e8f0",
      fontSize: 13,
    });
  }, [theme]);

  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'ISSUERTRADINGSYMBOL', headerName: 'Ticker', sortable: true, filter: true, width: 100, cellStyle: { fontWeight: 'bold', color: '#10b981', textAlign: undefined } },
    { field: 'ISSUERNAME', headerName: 'Company', sortable: true, filter: true, flex: 1 },
    { 
      field: 'FILING_DATE', 
      headerName: 'Date', 
      sortable: true, 
      filter: true, 
      width: 130,
      valueFormatter: (params) => {
        if (!params.value) return '';
        const date = new Date(params.value);
        return date.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
      }
    },
    { 
      field: 'total_non_deriv_value', 
      headerName: 'Value (USD)', 
      sortable: true, 
      filter: 'agNumberColumnFilter',
      width: 160,
      cellStyle: { textAlign: 'right' },
      headerClass: 'text-right-header',
      valueFormatter: (params) => params.value ? `$${params.value.toLocaleString()}` : '$0'
    },
    { 
      field: 'total_shares_acquired', 
      headerName: 'Shares', 
      sortable: true, 
      width: 130,
      cellStyle: { textAlign: 'right' },
      headerClass: 'text-right-header',
      valueFormatter: (params) => params.value ? params.value.toLocaleString() : '0'
    },
    { field: 'reporting_owner_ciks', headerName: 'Owners', flex: 1, filter: true },
  ], []);

  const defaultColDef = useMemo(() => ({
    resizable: true,
  }), []);

  if (!mounted) return <div className="w-full h-[600px] bg-secondary/20 animate-pulse rounded-xl" />;

  return (
    <div className="w-full h-[600px] rounded-xl overflow-hidden shadow-2xl border border-border">
      <AgGridReact
        theme={gridTheme}
        rowData={rowData}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        pagination={true}
        paginationPageSize={20}
        onGridReady={onGridReady}
        className="w-full h-full"
      />
    </div>
  );
}
