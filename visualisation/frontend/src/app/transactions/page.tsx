"use client";

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import Navbar from "@/components/Navbar";
import TransactionTable from "@/components/TransactionTable";
import DateRangePicker from "@/components/DateRangePicker";
import DirectorySearchBar from "@/components/DirectorySearchBar";
import { api } from "@/services/api";
import {
  loadStoredDateRange,
  saveStoredDateRange,
  defaultStoredDateRange,
} from "@/lib/dateRangeStorage";
import { transactionsQueryFromInput } from "@/lib/transactionsFilter";
import {
  ensureSearchDirectoryLoaded,
  loadSearchDirectoryCache,
} from "@/lib/searchDirectoryCache";
import { cn, formatIsoDateLabel } from "@/lib/utils";

const PAGE_SIZE = 50;

export default function TransactionsPage() {
  const [transactions, setTransactions] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchInput, setSearchInput] = useState("");
  const [dateRange, setDateRange] = useState(() => defaultStoredDateRange());
  const [appliedSearch, setAppliedSearch] = useState("");
  const [appliedRange, setAppliedRange] = useState(() => defaultStoredDateRange());
  const [page, setPage] = useState(1);
  const [totalRows, setTotalRows] = useState<number | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [tickerClose, setTickerClose] = useState<Map<string, number>>(() => new Map());

  useLayoutEffect(() => {
    const stored = loadStoredDateRange();
    if (stored) {
      setDateRange(stored);
      setAppliedRange(stored);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      await ensureSearchDirectoryLoaded();
      if (cancelled) return;
      const hit = loadSearchDirectoryCache();
      const m = new Map<string, number>();
      if (hit?.stocks) {
        for (const r of hit.stocks) {
          if (r.last_close != null && Number.isFinite(r.last_close)) {
            m.set(r.ticker.toUpperCase(), r.last_close);
          }
        }
      }
      setTickerClose(m);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleDateRange = useCallback((r: { start: string; end: string }) => {
    setDateRange(r);
  }, []);

  const appliedFilter = useMemo(
    () => transactionsQueryFromInput(appliedSearch),
    [appliedSearch]
  );

  const loadTransactions = useCallback(async () => {
    if (!appliedRange.start || !appliedRange.end) return;
    setLoading(true);
    try {
      const data = await api.getTransactions({
        startDate: appliedRange.start,
        endDate: appliedRange.end,
        ticker: appliedFilter.ticker,
        search: appliedFilter.search,
        page,
        page_size: PAGE_SIZE,
      });
      setTransactions(Array.isArray(data.data) ? (data.data as Record<string, unknown>[]) : []);
      const t = data.total;
      setTotalRows(typeof t === "number" && t >= 0 ? t : null);
      setHasMore(Boolean(data.has_more));
    } catch (e) {
      console.error("Failed to load transactions:", e);
    } finally {
      setLoading(false);
    }
  }, [appliedRange.start, appliedRange.end, appliedFilter.ticker, appliedFilter.search, page]);

  useEffect(() => {
    void loadTransactions();
  }, [loadTransactions]);

  const handleApply = useCallback(() => {
    saveStoredDateRange(dateRange);
    setAppliedSearch(searchInput.trim());
    setAppliedRange({ start: dateRange.start, end: dateRange.end });
    setPage(1);
  }, [dateRange, searchInput]);

  const totalPages =
    totalRows != null ? Math.max(1, Math.ceil(totalRows / PAGE_SIZE)) : null;

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      <main className="container mx-auto px-4 py-8 sm:px-8">
        <div className="flex flex-col lg:flex-row justify-between items-start mb-10 gap-6">
          <h1 className="text-4xl font-extrabold tracking-tight text-foreground sm:text-5xl">
            Detailed Transactions
          </h1>

          <div className="flex flex-col gap-3 w-full lg:w-auto lg:min-w-[32rem]">
            <div className="flex flex-col sm:flex-row flex-wrap items-stretch sm:items-center gap-3">
              <DirectorySearchBar value={searchInput} onChange={setSearchInput} className="w-full" />
              <DateRangePicker value={dateRange} onChange={handleDateRange} />
              <button
                type="button"
                onClick={handleApply}
                disabled={loading}
                className={cn(
                  "shrink-0 px-6 py-2.5 rounded-xl text-sm font-bold shadow transition-colors",
                  "bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:pointer-events-none"
                )}
              >
                {loading ? "Loading…" : "Apply"}
              </button>
            </div>
            <p className="text-xs text-muted-foreground">
              Set search and date range, then Apply. A symbol like <span className="font-mono">AAPL</span> lists{" "}
              <strong className="font-medium text-foreground">that ticker only</strong>. Names or phrases
              search across insiders and companies.
            </p>
          </div>
        </div>

        {loading ? (
          <div className="w-full h-[600px] bg-card animate-pulse rounded-2xl border border-border flex items-center justify-center">
            <p className="text-muted-foreground font-medium animate-bounce italic">Loading transactions...</p>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 px-1">
              <p className="text-sm text-muted-foreground">
                {totalRows != null ? (
                  <>
                    <span className="text-foreground font-medium tabular-nums">{totalRows.toLocaleString()}</span>{" "}
                    filing{totalRows === 1 ? "" : "s"} in range (by transaction date)
                  </>
                ) : appliedFilter.ticker ? (
                  <>
                    Ticker{" "}
                    <span className="font-mono text-foreground font-medium">{appliedFilter.ticker}</span> — page{" "}
                    <span className="text-foreground font-medium tabular-nums">{page}</span>
                    {hasMore ? " · more pages below" : " · end of results"}
                  </>
                ) : appliedSearch.trim() ? (
                  <>
                    Free-text search — page <span className="text-foreground font-medium tabular-nums">{page}</span>
                    {hasMore ? " · more pages below" : " · end of results"}
                  </>
                ) : (
                  <>
                    <span className="text-foreground font-medium tabular-nums">
                      {(totalRows ?? 0).toLocaleString()}
                    </span>{" "}
                    filings (all symbols, by transaction date)
                  </>
                )}
              </p>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  disabled={loading || page <= 1}
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  className={cn(
                    "inline-flex items-center gap-1 rounded-lg border border-border bg-card px-3 py-2 text-sm font-medium",
                    "hover:bg-muted/80 disabled:pointer-events-none disabled:opacity-40"
                  )}
                >
                  <ChevronLeft className="w-4 h-4" aria-hidden />
                  Previous
                </button>
                <span className="text-sm tabular-nums text-muted-foreground min-w-[8rem] text-center">
                  {totalPages != null ? (
                    <>
                      Page {page} / {totalPages}
                    </>
                  ) : (
                    <>Page {page}</>
                  )}
                </span>
                <button
                  type="button"
                  disabled={loading || !hasMore}
                  onClick={() => setPage((p) => p + 1)}
                  className={cn(
                    "inline-flex items-center gap-1 rounded-lg border border-border bg-card px-3 py-2 text-sm font-medium",
                    "hover:bg-muted/80 disabled:pointer-events-none disabled:opacity-40"
                  )}
                >
                  Next
                  <ChevronRight className="w-4 h-4" aria-hidden />
                </button>
              </div>
            </div>
            <div className="space-y-2">
              <p className="text-sm text-muted-foreground px-1">
                Transaction date range (applied):{" "}
                <span className="font-medium text-foreground">
                  {appliedRange.start && appliedRange.end ? (
                    <>
                      {formatIsoDateLabel(appliedRange.start)} – {formatIsoDateLabel(appliedRange.end)}
                    </>
                  ) : (
                    "—"
                  )}
                </span>
              </p>
              <div className="bg-card rounded-2xl border border-border shadow-lg overflow-hidden">
                <TransactionTable rowData={transactions} tickerClose={tickerClose} />
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
