"use client";

import React, { useEffect, useState, useCallback } from "react";
import Navbar from "@/components/Navbar";
import StatsGrid from "@/components/StatsGrid";
import { api } from "@/services/api";
import { Calendar, ChevronRight, ArrowUpCircle, ArrowDownCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface TopTx {
  ticker: string;
  company: string;
  value_m: number;
}

export default function Home() {
  const [stats, setStats] = useState({
    purchase_value_m: 0,
    purchase_count: 0,
    sales_value_m: 0,
    sales_count: 0,
  });
  const [topBuys, setTopBuys] = useState<TopTx[]>([]);
  const [topSells, setTopSells] = useState<TopTx[]>([]);
  const [activeTab, setActiveTab] = useState<"buys" | "sells">("buys");
  const [loading, setLoading] = useState(true);
  const [dateRange, setDateRange] = useState({ start: "", end: "" });

  const setPresetRange = useCallback((months: number) => {
    const end = new Date();
    const start = new Date();
    start.setMonth(start.getMonth() - months);
    setDateRange({
      start: start.toISOString().split("T")[0],
      end: end.toISOString().split("T")[0],
    });
  }, []);

  useEffect(() => { setPresetRange(6); }, [setPresetRange]);

  useEffect(() => {
    if (!dateRange.start || !dateRange.end) return;
    async function loadData() {
      setLoading(true);
      try {
        const [summaryData, topData] = await Promise.all([
          api.getSummary(dateRange.start, dateRange.end),
          api.getTopTransactions(dateRange.start, dateRange.end),
        ]);
        setStats(summaryData);
        setTopBuys(topData.top_buys || []);
        setTopSells(topData.top_sells || []);
      } catch (error) {
        console.error("Error loading dashboard data:", error);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, [dateRange]);

  const presets = [
    { label: "1 month", months: 1 },
    { label: "3 months", months: 3 },
    { label: "6 months", months: 6 },
    { label: "1 year", months: 12 },
  ];

  const activeItems = activeTab === "buys" ? topBuys : topSells;

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      <main className="container mx-auto px-4 py-8 sm:px-8">
        {/* Header + Date Picker */}
        <div className="flex flex-col lg:flex-row justify-between items-start mb-10 gap-8">
          <div className="max-w-2xl">
            <h1 className="text-4xl font-extrabold font-headline tracking-tight text-foreground sm:text-5xl">
              Market Activity Overview
            </h1>
            <p className="text-muted-foreground mt-4 text-base leading-relaxed max-w-xl">
              S&P 500 insider signals.
            </p>
          </div>

          <div className="w-full lg:w-auto bg-card p-4 rounded-2xl border border-border shadow-sm">
            <div className="flex items-center gap-3 mb-4 text-xs font-bold uppercase tracking-widest text-muted-foreground">
              <Calendar className="w-4 h-4" />
              Analyze Period
            </div>
            <div className="flex flex-wrap gap-2 mb-4">
              {presets.map((p) => (
                <button
                  key={p.label}
                  onClick={() => setPresetRange(p.months)}
                  className={cn(
                    "px-3 py-1.5 rounded-lg text-xs font-medium transition-all active:scale-95",
                    dateRange.start &&
                      new Date(dateRange.start).getMonth() ===
                      new Date(new Date().setMonth(new Date().getMonth() - p.months)).getMonth()
                      ? "bg-primary text-primary-foreground shadow-md"
                      : "bg-secondary text-secondary-foreground hover:bg-muted"
                  )}
                >
                  {p.label}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-3">
              <input
                type="date"
                value={dateRange.start}
                onChange={(e) => setDateRange((prev) => ({ ...prev, start: e.target.value }))}
                className="bg-background border border-border rounded-lg px-3 py-2 text-xs focus:ring-1 focus:ring-primary outline-none"
              />
              <ChevronRight className="w-4 h-4 text-muted-foreground" />
              <input
                type="date"
                value={dateRange.end}
                onChange={(e) => setDateRange((prev) => ({ ...prev, end: e.target.value }))}
                className="bg-background border border-border rounded-lg px-3 py-2 text-xs focus:ring-1 focus:ring-primary outline-none"
              />
            </div>
          </div>
        </div>

        {/* Cards grid — Market Volume | Top Transactions (side by side) */}
        {loading ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="h-72 bg-card animate-pulse rounded-2xl border border-border" />
            <div className="h-72 bg-card animate-pulse rounded-2xl border border-border" />
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Market Volume Activity */}
            <StatsGrid stats={stats} />

            {/* Top Transactions */}
            <div className="bg-card rounded-2xl border border-border shadow-md overflow-hidden flex flex-col">
              {/* Header */}
              <div className="px-6 py-4 border-b border-border flex items-center justify-between">
                <h3 className="text-sm font-bold text-muted-foreground uppercase tracking-wider">
                  Top Transactions
                </h3>
                <div className="flex items-center gap-1 bg-secondary rounded-xl p-1">
                  <button
                    onClick={() => setActiveTab("buys")}
                    className={cn(
                      "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold transition-all",
                      activeTab === "buys"
                        ? "bg-emerald-500 text-white shadow"
                        : "text-muted-foreground hover:text-foreground"
                    )}
                  >
                    <ArrowUpCircle className="w-3.5 h-3.5" />
                    Top Buys
                  </button>
                  <button
                    onClick={() => setActiveTab("sells")}
                    className={cn(
                      "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold transition-all",
                      activeTab === "sells"
                        ? "bg-rose-500 text-white shadow"
                        : "text-muted-foreground hover:text-foreground"
                    )}
                  >
                    <ArrowDownCircle className="w-3.5 h-3.5" />
                    Top Sells
                  </button>
                </div>
              </div>

              {/* Rows */}
              <div className="flex-1 flex flex-col justify-center">
                {activeItems.length === 0 ? (
                  <div className="p-8 text-center text-muted-foreground text-sm">
                    No data available for the selected period.
                  </div>
                ) : (
                  activeItems.map((tx, i) => (
                    <div
                      key={tx.ticker}
                      className="flex items-center justify-between px-6 py-5 border-b border-border last:border-0 hover:bg-primary/5 transition-colors group"
                    >
                      <div className="flex items-center gap-4">
                        <span className="text-2xl font-black text-muted-foreground/30 w-6 text-center group-hover:text-primary/40 transition-colors">
                          {i + 1}
                        </span>
                        <div>
                          <p className="text-lg font-black text-primary tracking-tighter">{tx.ticker}</p>
                          <p className="text-xs text-muted-foreground truncate max-w-[200px]">{tx.company}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className={cn(
                          "text-xl font-bold font-headline",
                          activeTab === "buys" ? "text-emerald-400" : "text-rose-400"
                        )}>
                          ${tx.value_m.toFixed(2)}M
                        </p>
                        <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                          {activeTab === "buys" ? "Purchased" : "Sold"}
                        </p>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
