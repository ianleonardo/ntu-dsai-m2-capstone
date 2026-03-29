"use client";

import React, { useEffect, useLayoutEffect, useState, useCallback } from "react";
import Navbar from "@/components/Navbar";
import StatsGrid from "@/components/StatsGrid";
import DateRangePicker from "@/components/DateRangePicker";
import { ArrowUpCircle, ArrowDownCircle } from "lucide-react";
import { cn, formatIsoDateLabel, formatMillionsDisplay } from "@/lib/utils";
import {
  loadStoredDateRange,
  saveStoredDateRange,
  defaultStoredDateRange,
} from "@/lib/dateRangeStorage";
import { readOverviewCache, type TopTx, type OverviewBundle } from "@/lib/overviewCache";
import { loadOverviewBundle } from "@/lib/overviewFetch";

const EMPTY_STATS: OverviewBundle["stats"] = {
  purchase_value_m: 0,
  purchase_count: 0,
  sales_value_m: 0,
  sales_count: 0,
};

export default function Home() {
  const [dateRange, setDateRange] = useState(() => defaultStoredDateRange());
  const [stats, setStats] = useState(EMPTY_STATS);
  const [topBuys, setTopBuys] = useState<TopTx[]>([]);
  const [topSells, setTopSells] = useState<TopTx[]>([]);
  const [activeTab, setActiveTab] = useState<"buys" | "sells">("buys");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useLayoutEffect(() => {
    const stored = loadStoredDateRange();
    if (stored) {
      // Apply persisted range before passive effects; avoids hydration mismatch vs reading storage in useState.
      // eslint-disable-next-line react-hooks/set-state-in-effect -- intentional layout-time sync from localStorage
      setDateRange(stored);
    }
  }, []);

  const handleDateRange = useCallback(
    (r: { start: string; end: string }) => {
      setDateRange(r);
      saveStoredDateRange(r);
    },
    []
  );

  useEffect(() => {
    if (!dateRange.start || !dateRange.end) return;

    const cached = readOverviewCache(dateRange.start, dateRange.end);
    if (cached) {
      queueMicrotask(() => {
        setStats(cached.stats);
        setTopBuys(cached.topBuys);
        setTopSells(cached.topSells);
        setError(null);
        setLoading(false);
      });
      return;
    }

    let cancelled = false;
    queueMicrotask(() => {
      if (!cancelled) {
        setLoading(true);
        setError(null);
      }
    });
    loadOverviewBundle(dateRange.start, dateRange.end)
      .then((bundle) => {
        if (cancelled) return;
        setStats(bundle.stats);
        setTopBuys(bundle.topBuys);
        setTopSells(bundle.topSells);
      })
      .catch((e) => {
        console.error("Error loading dashboard data:", e);
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to load dashboard data. Please check your connection.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dateRange.start, dateRange.end]);

  const activeItems = activeTab === "buys" ? topBuys : topSells;

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      <main className="container mx-auto px-4 py-8 sm:px-8">

        {/* Header + Picker row */}
        <div className="flex flex-col lg:flex-row justify-between items-start mb-10 gap-6">
          <div>
            <h1 className="text-4xl font-extrabold tracking-tight text-foreground sm:text-5xl">
              Market Activity Overview
            </h1>
            <p className="text-muted-foreground mt-3 text-base max-w-xl">
              For S&P 500 constituents, metrics and top lists use reported{" "}
              <span className="text-foreground/90 font-medium">transaction dates</span> within the analysis period:{" "}
              <span className="text-primary font-semibold">
                {dateRange.start ? formatIsoDateLabel(dateRange.start) : "—"}
              </span>
              {" "}to{" "}
              <span className="text-primary font-semibold">
                {dateRange.end ? formatIsoDateLabel(dateRange.end) : "—"}
              </span>
            </p>
          </div>
          <div className="pt-1">
            <DateRangePicker value={dateRange} onChange={handleDateRange} />
          </div>
        </div>

        {/* Cards grid */}
        {loading ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="h-72 bg-card animate-pulse rounded-2xl border border-border" />
            <div className="h-72 bg-card animate-pulse rounded-2xl border border-border" />
          </div>
        ) : error ? (
          <div className="w-full h-[300px] bg-card rounded-2xl border border-destructive/30 flex flex-col items-center justify-center p-8 text-center">
            <div className="bg-destructive/10 p-4 rounded-full mb-4">
              <span className="text-destructive text-2xl">⚠️</span>
            </div>
            <h3 className="text-foreground font-bold text-xl mb-1">Unable to load dashboard data</h3>
            <p className="text-muted-foreground mb-6 max-w-md">{error}</p>
            <button 
              onClick={() => {
                setDateRange({ ...dateRange }); // Trigger useEffect
              }}
              className="px-8 py-2.5 bg-primary text-primary-foreground rounded-xl font-bold hover:bg-primary/90 transition-colors"
            >
              Try Again
            </button>
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Market Volume Activity */}
            <StatsGrid stats={stats} />

            {/* Top Transactions */}
            <div className="bg-card rounded-2xl border border-border shadow-md overflow-hidden flex flex-col">
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
                          "text-xl font-bold",
                          activeTab === "buys" ? "text-emerald-400" : "text-rose-400"
                        )}>
                          ${formatMillionsDisplay(tx.value_m)}M
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
