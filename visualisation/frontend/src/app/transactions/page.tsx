"use client";

import React, { useEffect, useState, useCallback } from "react";
import Navbar from "@/components/Navbar";
import TransactionTable from "@/components/TransactionTable";
import { api } from "@/services/api";
import { Search, Calendar, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

export default function TransactionsPage() {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  
  // Date states
  const [dateRange, setDateRange] = useState({
    start: "",
    end: ""
  });

  const setPresetRange = useCallback((months: number) => {
    const end = new Date();
    const start = new Date();
    start.setMonth(start.getMonth() - months);
    
    const startStr = start.toISOString().split('T')[0];
    const endStr = end.toISOString().split('T')[0];
    
    setDateRange({ start: startStr, end: endStr });
  }, []);

  // Initial load: 6 months
  useEffect(() => {
    setPresetRange(6);
  }, [setPresetRange]);

  useEffect(() => {
    if (!dateRange.start || !dateRange.end) return;

    async function loadTransactions() {
      setLoading(true);
      try {
        const data = await api.getTransactions({ 
          startDate: dateRange.start, 
          endDate: dateRange.end,
          ticker: searchTerm || undefined,
          page: 1
        });
        setTransactions(data.data || []);
      } catch (error) {
        console.error("Failed to load transactions:", error);
      } finally {
        setLoading(false);
      }
    }
    loadTransactions();
  }, [dateRange, searchTerm]);

  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      
      <main className="container mx-auto px-4 py-8 sm:px-8">
        <div className="flex flex-col lg:flex-row justify-between items-start mb-10 gap-8">
          <div className="max-w-xl">
            <h1 className="text-4xl font-extrabold font-headline tracking-tight text-foreground sm:text-5xl">
              Detailed Transactions
            </h1>
          </div>

          <div className="w-full lg:w-auto flex flex-col sm:flex-row gap-4">
            {/* Search Input */}
            <div className="relative group min-w-[300px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground w-4 h-4 group-focus-within:text-primary transition-colors" />
              <input 
                type="text"
                placeholder="Search Ticker..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full bg-card border border-border rounded-xl pl-10 pr-4 py-2 text-sm focus:ring-1 focus:ring-primary outline-none transition-all shadow-sm"
              />
            </div>

            {/* Date Picker */}
            <div className="bg-card p-3 rounded-xl border border-border shadow-sm flex flex-col sm:flex-row items-center gap-3">
              <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-widest text-muted-foreground px-1">
                <Calendar className="w-4 h-4" />
                Period
              </div>
              <div className="flex items-center gap-2">
                <input 
                  type="date" 
                  value={dateRange.start}
                  onChange={(e) => setDateRange(prev => ({ ...prev, start: e.target.value }))}
                  className="bg-background border border-border rounded-lg px-2 py-1 text-xs focus:ring-1 focus:ring-primary outline-none"
                />
                <ChevronRight className="w-4 h-4 text-muted-foreground" />
                <input 
                  type="date" 
                  value={dateRange.end}
                  onChange={(e) => setDateRange(prev => ({ ...prev, end: e.target.value }))}
                  className="bg-background border border-border rounded-lg px-2 py-1 text-xs focus:ring-1 focus:ring-primary outline-none"
                />
              </div>
            </div>
          </div>
        </div>

        {loading ? (
          <div className="w-full h-[600px] bg-card animate-pulse rounded-2xl border border-border flex items-center justify-center">
            <p className="text-muted-foreground font-medium animate-bounce italic">Syncing high-fidelity transaction logic...</p>
          </div>
        ) : (
          <div className="bg-card rounded-2xl border border-border shadow-lg overflow-hidden">
            <TransactionTable rowData={transactions} />
          </div>
        )}
      </main>
    </div>
  );
}
