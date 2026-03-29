"use client";

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Navbar from "@/components/Navbar";
import UnifiedFilterBar from "@/components/UnifiedFilterBar";
import { cn, formatIsoDateLabel } from "@/lib/utils";
import { api } from "@/services/api";
import {
  loadStoredDateRange,
  saveStoredDateRange,
  defaultStoredDateRange,
} from "@/lib/dateRangeStorage";
import {
  defaultClustersUiPersisted,
  loadClustersUiPersisted,
  saveClustersUiPersisted,
  type ClusterSortKey,
} from "@/lib/clustersUiStorage";
import { loadUnifiedFilters, sizeTierToValues, UnifiedFiltersState, DEFAULT_FILTERS } from "@/lib/unifiedFiltersStorage";
import { transactionsQueryFromInput } from "@/lib/transactionsFilter";
import {
  ensureSearchDirectoryLoaded,
  loadSearchDirectoryCache,
} from "@/lib/searchDirectoryCache";
import {
  lookupTickerClose,
  returnVsTxnPricePct,
  formatReturnPctDisplay,
  returnPctColorClass,
  formatUsdPerShare,
  formatUsdSmart,
  txnSizeBadge,
} from "@/lib/transactionRowModel";
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronDown, ChevronUp, Flame, Info } from "lucide-react";
import CandlestickModal from "@/components/CandlestickModal";

type ClusterRow = {
  ticker: string;
  company: string;
  issuer_gics_sector?: string;
  filing_count: number;
  insider_count: number;
  week_start: string;
  first_trans: string;
  last_trans: string;
  last_filing_date: string;
  cluster_value: number;
  cluster_shares?: number;
  implied_price_per_share?: number | null;
  roles: string;
  titles: string;
};

type BreakdownRow = {
  insider_name: string;
  role: string;
  trans_date: string;
  amount_usd: number;
};

function isoDay(iso: string) {
  return iso.length >= 10 ? iso.slice(0, 10) : iso;
}

/** DD MMM yyyy (same as applied range line; no locale drift). */
function fmtClusterDate(d: string) {
  return formatIsoDateLabel(isoDay(d));
}

function windowDays(first: string, last: string) {
  const a = new Date(first).getTime();
  const b = new Date(last).getTime();
  return Math.max(1, Math.round((b - a) / 86400000) + 1);
}

/** Whole calendar days from filing date (local) to today. */
function filingDaysAgo(iso: string): number | null {
  const day = isoDay(iso);
  const [y, m, d] = day.split("-").map((x) => parseInt(x, 10));
  if (!y || !m || !d) return null;
  const filed = new Date(y, m - 1, d);
  const t = new Date();
  const today = new Date(t.getFullYear(), t.getMonth(), t.getDate());
  const diff = Math.floor((today.getTime() - filed.getTime()) / 86400000);
  if (!Number.isFinite(diff)) return null;
  return Math.max(0, diff);
}

function formatFilingDaysAgo(iso: string) {
  const n = filingDaysAgo(iso);
  if (n == null) return "—";
  if (n === 0) return "Today";
  if (n === 1) return "1 day ago";
  return `${n} days ago`;
}

function clusterKey(r: ClusterRow) {
  return `${(r.ticker || "").trim().toUpperCase()}|${isoDay(r.week_start)}`;
}

function clusterSectorChip(sector?: string): string | null {
  const s = (sector || "").trim();
  if (!s) return null;
  const w = s.split(/\s+/)[0] || s;
  return w.length > 12 ? `${w.slice(0, 11)}…` : w;
}

function execCeoCfo(roles: string, titles: string) {
  const s = `${titles} ${roles}`;
  return {
    ceo: /\bCEO\b|Chief Executive/i.test(s),
    cfo: /\bCFO\b|Chief Financial/i.test(s),
  };
}

function clusterRetPct(r: ClusterRow, tickerClose: ReadonlyMap<string, number>): number | null {
  const sym = (r.ticker || "").trim().toUpperCase();
  const lastClose = lookupTickerClose(tickerClose, sym);
  const cost = r.implied_price_per_share;
  return returnVsTxnPricePct(lastClose, cost ?? null);
}

function clusterSortValue(
  r: ClusterRow,
  key: ClusterSortKey,
  tickerClose: ReadonlyMap<string, number>
): string | number | null {
  switch (key) {
    case "ticker":
      return (r.ticker || "").toUpperCase();
    case "signal":
      return r.insider_count;
    case "window":
      return windowDays(r.first_trans, r.last_trans);
    case "value":
      return r.cluster_value;
    case "return":
      return clusterRetPct(r, tickerClose);
    case "filed": {
      const t = Date.parse(isoDay(r.last_filing_date) + "T12:00:00");
      return Number.isFinite(t) ? t : 0;
    }
    default:
      return 0;
  }
}

function compareClusterRows(
  a: ClusterRow,
  b: ClusterRow,
  key: ClusterSortKey,
  dir: "asc" | "desc",
  tickerClose: ReadonlyMap<string, number>
): number {
  const mul = dir === "asc" ? 1 : -1;
  const va = clusterSortValue(a, key, tickerClose);
  const vb = clusterSortValue(b, key, tickerClose);
  if (key === "return") {
    const na = va == null || (typeof va === "number" && !Number.isFinite(va));
    const nb = vb == null || (typeof vb === "number" && !Number.isFinite(vb));
    if (na && nb) return 0;
    if (na) return 1;
    if (nb) return -1;
    return ((va as number) - (vb as number)) * mul;
  }
  if (typeof va === "number" && typeof vb === "number") return (va - vb) * mul;
  return String(va).localeCompare(String(vb)) * mul;
}

function ClusterTh({
  label,
  info,
  sortKey,
  activeKey,
  dir,
  onSort,
  className,
}: {
  label: string;
  info?: string;
  sortKey: ClusterSortKey;
  activeKey: ClusterSortKey;
  dir: "asc" | "desc";
  onSort: (k: ClusterSortKey) => void;
  className?: string;
}) {
  const active = activeKey === sortKey;
  return (
    <th
      className={cn(
        "px-1 py-2 text-left align-bottom font-bold text-xs leading-tight uppercase tracking-wide text-muted-foreground",
        className
      )}
    >
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className="inline-flex items-center gap-0.5 hover:text-foreground transition-colors text-left max-w-full"
      >
        <span className="break-words hyphens-auto">{label}</span>
        {info ? (
          <span title={info} className="inline-flex shrink-0" aria-label={info}>
            <Info className="w-3 h-3 opacity-60" />
          </span>
        ) : null}
        {active ? (
          dir === "asc" ? (
            <ArrowUp className="w-3.5 h-3.5 text-primary shrink-0" />
          ) : (
            <ArrowDown className="w-3.5 h-3.5 text-primary shrink-0" />
          )
        ) : (
          <ArrowUpDown className="w-3.5 h-3.5 opacity-40 shrink-0" />
        )}
      </button>
    </th>
  );
}

export default function ClustersPage() {
  const ui0 = defaultClustersUiPersisted();
  const [side, setSide] = useState<"buy" | "sell">(ui0.side);
  const [minFilings, setMinFilings] = useState(ui0.minFilings);
  const [dateRange, setDateRange] = useState(() => defaultStoredDateRange());
  const [appliedFilters, setAppliedFilters] = useState<UnifiedFiltersState>(DEFAULT_FILTERS);
  const [appliedRange, setAppliedRange] = useState(() => defaultStoredDateRange());
  const [rows, setRows] = useState<ClusterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tickerClose, setTickerClose] = useState<Map<string, number>>(() => new Map());
  const [priceBelowCostOnly, setPriceBelowCostOnly] = useState(ui0.priceBelowCostOnly);
  const [sortKey, setSortKey] = useState<ClusterSortKey>(ui0.sortKey);
  const [sortDir, setSortDir] = useState<"asc" | "desc">(ui0.sortDir);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [breakdownByKey, setBreakdownByKey] = useState<Map<string, BreakdownRow[]>>(() => new Map());
  const breakdownRef = useRef(breakdownByKey);
  breakdownRef.current = breakdownByKey;
  const [breakdownLoadingKey, setBreakdownLoadingKey] = useState<string | null>(null);
  const [selectedChart, setSelectedChart] = useState<{ ticker: string; transDate: string } | null>(null);

  useLayoutEffect(() => {
    const stored = loadStoredDateRange();
    if (stored) {
      setDateRange(stored);
      setAppliedRange(stored);
    }
    const p = loadClustersUiPersisted();
    setSide(p.side);
    setMinFilings(p.minFilings);
    setPriceBelowCostOnly(p.priceBelowCostOnly);
    setSortKey(p.sortKey);
    setSortDir(p.sortDir);
    setAppliedFilters(loadUnifiedFilters());
  }, []);

  useEffect(() => {
    saveClustersUiPersisted({
      side,
      minFilings,
      priceBelowCostOnly,
      searchInput: "",
      appliedSearch: "",
      sortKey,
      sortDir,
    });
  }, [side, minFilings, priceBelowCostOnly, sortKey, sortDir]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
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
      } catch (e) {
        console.warn("Failed to warm search directory:", e);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleDateRange = useCallback((r: { start: string; end: string }) => {
    setDateRange(r);
  }, []);

  const appliedFilter = useMemo(
    () => transactionsQueryFromInput(appliedFilters.search),
    [appliedFilters.search]
  );

  // Stable serialized keys so React detects changes when array contents change
  const clusterSectorKey = appliedFilters.sector.join(",");
  const clusterRoleKey = appliedFilters.role.join(",");

  const loadClusters = useCallback(async () => {
    if (!appliedRange.start || !appliedRange.end) return;
    setLoading(true);
    setError(null);
    try {
      const sizeReq = sizeTierToValues(appliedFilters.size);
      const res = await api.getClusters({
        side,
        startDate: appliedRange.start,
        endDate: appliedRange.end,
        min_filings: minFilings,
        limit: 150,
        search: appliedFilter.search,
        sector: appliedFilters.sector.length === 0 ? undefined : appliedFilters.sector,
        role: appliedFilters.role.length === 0 ? undefined : appliedFilters.role,
        min_value: sizeReq.min_value,
        max_value: sizeReq.max_value,
      });
      const data = res.data;
      setRows(Array.isArray(data) ? (data as ClusterRow[]) : []);
      setBreakdownByKey(new Map());
      setExpandedKey(null);
    } catch (e) {
      console.error(e);
      setRows([]);
      setError(e instanceof Error ? e.message : "An unexpected error occurred while fetching clusters.");
    } finally {
      setLoading(false);
    }
  }, [
    side,
    appliedRange.start,
    appliedRange.end,
    minFilings,
    appliedFilter.search,
    clusterSectorKey,
    clusterRoleKey,
    appliedFilters.size,
  ]);

  useEffect(() => {
    void loadClusters();
  }, [loadClusters]);

  const handleApply = useCallback(() => {
    saveStoredDateRange(dateRange);
    setAppliedFilters(loadUnifiedFilters());
    setAppliedRange({ start: dateRange.start, end: dateRange.end });
  }, [dateRange]);

  const onSort = useCallback(
    (k: ClusterSortKey) => {
      if (k === sortKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      else {
        setSortKey(k);
        setSortDir(k === "ticker" ? "asc" : k === "filed" ? "desc" : "desc");
      }
    },
    [sortKey]
  );

  const loadBreakdown = useCallback(
    async (row: ClusterRow) => {
      const key = clusterKey(row);
      if (breakdownRef.current.has(key)) return;
      setBreakdownLoadingKey(key);
      try {
        const res = await api.getClusterBreakdown({
          side,
          ticker: row.ticker.trim(),
          weekStart: isoDay(row.week_start),
          startDate: appliedRange.start,
          endDate: appliedRange.end,
        });
        const raw = res.data;
        const list: BreakdownRow[] = Array.isArray(raw)
          ? raw.map((x) => ({
              insider_name: String(x?.insider_name ?? ""),
              role: String(x?.role ?? ""),
              trans_date: String(x?.trans_date ?? ""),
              amount_usd: typeof x?.amount_usd === "number" ? x.amount_usd : Number(x?.amount_usd) || 0,
            }))
          : [];
        setBreakdownByKey((m) => new Map(m).set(key, list));
      } catch (e) {
        console.error(e);
        setBreakdownByKey((m) => new Map(m).set(key, []));
      } finally {
        setBreakdownLoadingKey(null);
      }
    },
    [appliedRange.start, appliedRange.end, side]
  );

  const toggleAnalyze = useCallback(
    (row: ClusterRow) => {
      const key = clusterKey(row);
      if (expandedKey === key) {
        setExpandedKey(null);
        return;
      }
      setExpandedKey(key);
      void loadBreakdown(row);
    },
    [expandedKey, loadBreakdown]
  );

  const displayRows = useMemo(() => {
    if (!priceBelowCostOnly) return rows;
    return rows.filter((r) => {
      const t = (r.ticker || "").trim().toUpperCase();
      const cur = lookupTickerClose(tickerClose, t);
      const cost = r.implied_price_per_share;
      if (cur == null || cost == null || cost <= 0) return false;
      return cur < cost - 1e-9;
    });
  }, [rows, priceBelowCostOnly, tickerClose]);

  const sortedRows = useMemo(() => {
    const arr = [...displayRows];
    arr.sort((a, b) => compareClusterRows(a, b, sortKey, sortDir, tickerClose));
    return arr;
  }, [displayRows, sortKey, sortDir, tickerClose]);

  const colCount = 7;

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      <main className="container mx-auto px-4 py-8 sm:px-8">
        <div className="flex flex-col lg:flex-row justify-between items-start mb-10 gap-6">
          <div>
            <h1 className="text-4xl font-extrabold tracking-tight text-foreground sm:text-5xl mb-2">
              Insider clusters
            </h1>
            <p className="text-muted-foreground text-sm max-w-2xl">
              Insider clusters occur when multiple insiders (e.g., executives, directors) buy or sell company stock during the same narrow timeframe.
              Significant <strong className="font-medium text-foreground">cluster buying </strong>often signals strong collective confidence, 
              while <strong className="font-medium text-foreground">cluster selling </strong>may indicate shared concerns. 
              This aggregated activity is considered more meaningful than isolated transactions.
            </p>
          </div>

          <UnifiedFilterBar 
            dateRange={dateRange}
            onDateRangeChange={handleDateRange}
            onApply={handleApply}
            loading={loading}
          />
        </div>

        <div className="flex flex-wrap items-center gap-3 mb-6">
          <div className="flex rounded-xl border border-border bg-card p-1">
            <button
              type="button"
              onClick={() => setSide("buy")}
              className={cn(
                "px-4 py-2 rounded-lg text-sm font-bold transition-all",
                side === "buy" ? "bg-emerald-600 text-white shadow" : "text-muted-foreground hover:text-foreground"
              )}
            >
              Cluster Buys
            </button>
            <button
              type="button"
              onClick={() => setSide("sell")}
              className={cn(
                "px-4 py-2 rounded-lg text-sm font-bold transition-all",
                side === "sell" ? "bg-blue-600 text-white shadow" : "text-muted-foreground hover:text-foreground"
              )}
            >
              Cluster Sells
            </button>
          </div>

          <label
            className={cn(
              "flex items-center gap-2 text-sm font-medium select-none border border-border rounded-xl px-3 py-2 bg-muted/30 cursor-pointer",
              priceBelowCostOnly && "border-orange-500/50 bg-orange-500/10"
            )}
            title="Show only rows where latest close &lt; cluster implied $/share (cluster value ÷ shares on this side)."
          >
            <input
              type="checkbox"
              className="rounded border-border"
              checked={priceBelowCostOnly}
              onChange={(e) => setPriceBelowCostOnly(e.target.checked)}
            />
            <span className="flex items-center gap-1">
              Price &lt; Cost <Flame className="w-3.5 h-3.5 text-orange-500" />
            </span>
          </label>

          <div className="flex gap-1 items-center">
            <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground mr-1">Cluster size</span>
            {[2, 3, 5].map((n) => (
              <button
                key={n}
                type="button"
                onClick={() => setMinFilings(n)}
                className={cn(
                  "px-3 py-1.5 rounded-lg text-xs font-bold border",
                  minFilings === n
                    ? "border-primary bg-primary/10 text-primary"
                    : "border-border bg-card text-muted-foreground"
                )}
              >
                {n}+
              </button>
            ))}
          </div>
        </div>

        {loading ? (
          <div className="w-full min-h-[420px] bg-card animate-pulse rounded-2xl border border-border flex items-center justify-center">
            <p className="text-muted-foreground font-medium animate-bounce italic">Loading clusters…</p>
          </div>
        ) : error ? (
          <div className="w-full h-[400px] bg-card rounded-2xl border border-destructive/30 flex flex-col items-center justify-center p-8 text-center">
            <div className="bg-destructive/10 p-4 rounded-full mb-4">
              <span className="text-destructive text-2xl">⚠️</span>
            </div>
            <h3 className="text-foreground font-bold text-xl mb-2">Failed to load clusters</h3>
            <p className="text-muted-foreground mb-6 max-w-md">{error}</p>
            <button 
              onClick={() => void loadClusters()}
              className="px-6 py-2 bg-primary text-primary-foreground rounded-xl font-bold hover:bg-primary/90 transition-colors"
            >
              Try Again
            </button>
          </div>
        ) : (
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

            <div className="rounded-2xl border border-border bg-card overflow-hidden shadow-lg">
              <div className="overflow-x-auto">
                <table className="w-full table-fixed text-[11px] leading-snug border-collapse">
                  <thead>
                    <tr className="border-b border-border bg-muted/40 text-left">
                      <ClusterTh label="Ticker" sortKey="ticker" activeKey={sortKey} dir={sortDir} onSort={onSort} className="pl-2" />
                      <ClusterTh
                        label="Signal"
                        info="Distinct reporting owners (RPTOWNERCIK) in the cluster; only clusters with ≥2 insiders are listed. CEO/CFO chips use aggregated titles and roles."
                        sortKey="signal"
                        activeKey={sortKey}
                        dir={sortDir}
                        onSort={onSort}
                      />
                      <ClusterTh label="Window" sortKey="window" activeKey={sortKey} dir={sortDir} onSort={onSort} />
                      <ClusterTh
                        label="Value"
                        sortKey="value"
                        activeKey={sortKey}
                        dir={sortDir}
                        onSort={onSort}
                        className="text-right [&_button]:w-full [&_button]:justify-end"
                      />
                      <ClusterTh
                        label="Price / cost"
                        info="Latest close vs implied cluster $/share; return = (curr − cost) ÷ cost."
                        sortKey="return"
                        activeKey={sortKey}
                        dir={sortDir}
                        onSort={onSort}
                        className="text-right [&_button]:w-full [&_button]:justify-end pr-2"
                      />
                      <ClusterTh
                        label="Filed"
                        info="Calendar days since the latest SEC filing date among filings in this cluster (local date)."
                        sortKey="filed"
                        activeKey={sortKey}
                        dir={sortDir}
                        onSort={onSort}
                      />
                      <th className="px-1 py-2 text-right align-bottom font-bold text-xs uppercase tracking-wide text-muted-foreground pr-2">
                        Action
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.length === 0 && (
                      <tr>
                        <td colSpan={colCount} className="px-4 py-16 text-center text-muted-foreground text-sm">
                          No clusters for this window. Clusters need at least two distinct insiders (by CIK) and your
                          minimum filing count — try widening the date range, clearing search, or lowering cluster size.
                        </td>
                      </tr>
                    )}
                    {rows.length > 0 && displayRows.length === 0 && (
                      <tr>
                        <td colSpan={colCount} className="px-4 py-16 text-center text-muted-foreground text-sm">
                          No rows match Price &lt; Cost. Clear the filter or widen the date range.
                        </td>
                      </tr>
                    )}
                    {sortedRows.map((r, i) => {
                      const wd = windowDays(r.first_trans, r.last_trans);
                      const valueCls = side === "sell" ? "text-rose-500" : "text-emerald-500";
                      const sym = (r.ticker || "").trim().toUpperCase();
                      const lastClose = lookupTickerClose(tickerClose, sym);
                      const cost = r.implied_price_per_share;
                      const retPct = returnVsTxnPricePct(lastClose, cost ?? null);
                      const badges = execCeoCfo(r.roles, r.titles ?? "");
                      const sizeLabel = r.cluster_value > 0 ? txnSizeBadge(r.cluster_value) : "";
                      const sector = clusterSectorChip(r.issuer_gics_sector);
                      const key = clusterKey(r);
                      const open = expandedKey === key;
                      const breakdown = breakdownByKey.get(key);
                      const bdLoading = breakdownLoadingKey === key;

                      return (
                        <React.Fragment key={`${r.ticker}-${r.week_start}-${i}`}>
                          <tr className="border-b border-border hover:bg-muted/30">
                            <td className="px-2 py-3 pl-2">
                              <button
                                type="button"
                                onClick={() => {
                                  if (r.ticker && r.last_trans) {
                                    setSelectedChart({
                                      ticker: r.ticker.trim(),
                                      transDate: new Date(isoDay(r.last_trans) + "T12:00:00Z").toISOString(),
                                    });
                                  }
                                }}
                                className="font-black text-primary text-sm sm:text-base hover:underline text-left overflow-hidden text-ellipsis"
                                title={`View ${r.ticker} chart`}
                              >
                                {r.ticker}
                              </button>
                              <div className="text-[10px] text-muted-foreground truncate max-w-[140px]">{r.company}</div>
                              {sector ? (
                                <div className="flex flex-wrap gap-0.5 mt-1">
                                  <span className="text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded-full border border-primary/30 text-primary/90 truncate max-w-full">
                                    {sector}
                                  </span>
                                </div>
                              ) : null}
                            </td>
                            <td className="px-2 py-3">
                              <div className="font-semibold text-foreground">
                                {r.insider_count} Insider{r.insider_count === 1 ? "" : "s"}
                              </div>
                              <div className="flex flex-wrap gap-1 mt-1">
                                {badges.ceo && (
                                  <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-violet-500/20 text-violet-300 border border-violet-500/30">
                                    CEO
                                  </span>
                                )}
                                {badges.cfo && (
                                  <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-sky-500/20 text-sky-300 border border-sky-500/30">
                                    CFO
                                  </span>
                                )}
                              </div>
                            </td>
                            <td className="px-2 py-3">
                              <div className="font-medium">{wd} days</div>
                              <div className="text-[10px] text-muted-foreground">
                                {fmtClusterDate(r.first_trans)} – {fmtClusterDate(r.last_trans)}
                              </div>
                            </td>
                            <td className={cn("px-2 py-3 text-right tabular-nums", valueCls)}>
                              <div className="font-bold">{formatUsdSmart(r.cluster_value)}</div>
                              {sizeLabel ? (
                                <div className="mt-0.5 flex justify-end">
                                  <span className="text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded-full border border-border bg-secondary/60 text-muted-foreground">
                                    {sizeLabel}
                                  </span>
                                </div>
                              ) : null}
                            </td>
                            <td className="px-2 py-3 text-right align-top tabular-nums pr-2">
                              <div className="font-bold text-foreground">
                                {lastClose != null ? formatUsdPerShare(lastClose) : "—"}
                              </div>
                              <div className="text-[10px] text-muted-foreground mt-0.5">
                                Cost {cost != null && cost > 0 ? formatUsdPerShare(cost) : "—"}
                              </div>
                              <div className={cn("text-[10px] font-semibold mt-0.5", returnPctColorClass(retPct))}>
                                {formatReturnPctDisplay(retPct)}
                              </div>
                            </td>
                            <td className="px-2 py-3 text-[10px]">
                              <div className="font-semibold text-foreground">{formatFilingDaysAgo(r.last_filing_date)}</div>
                              <div className="text-muted-foreground mt-0.5 tabular-nums">
                                {r.last_filing_date ? fmtClusterDate(r.last_filing_date) : "—"}
                              </div>
                            </td>
                            <td className="px-2 py-3 text-right pr-2">
                              <button
                                type="button"
                                onClick={() => toggleAnalyze(r)}
                                className="text-primary font-bold text-[10px] sm:text-xs inline-flex items-center gap-1 hover:underline"
                              >
                                Analyze
                                {open ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                              </button>
                            </td>
                          </tr>
                          {open && (
                            <tr className="bg-muted/20 border-b border-border">
                              <td colSpan={colCount} className="p-0">
                                <div className="border-l-4 border-primary/60 bg-muted/30 px-4 py-4 ml-1">
                                  <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground mb-3">
                                    Cluster breakdown
                                  </p>
                                  {bdLoading && (
                                    <p className="text-sm text-muted-foreground italic py-4">Loading breakdown…</p>
                                  )}
                                  {!bdLoading && breakdown && breakdown.length === 0 && (
                                    <p className="text-sm text-muted-foreground py-2">
                                      No owner-level rows for this cluster window. If the table is missing, run{" "}
                                      <code className="text-xs bg-background px-1 rounded">dbt build</code> so{" "}
                                      <code className="text-xs bg-background px-1 rounded">dim_sec_reporting_owner</code>{" "}
                                      exists in BigQuery. Otherwise check the browser console for API errors (failed
                                      requests are shown as empty here).
                                    </p>
                                  )}
                                  {!bdLoading && breakdown && breakdown.length > 0 && (
                                    <div className="rounded-lg border border-border bg-card overflow-hidden">
                                      <table className="w-full text-[11px]">
                                        <thead>
                                          <tr className="border-b border-border bg-muted/50 text-left">
                                            <th className="px-3 py-2 font-bold text-xs uppercase tracking-wide text-muted-foreground">
                                              Insider name
                                            </th>
                                            <th className="px-3 py-2 font-bold text-xs uppercase tracking-wide text-muted-foreground">
                                              Role
                                            </th>
                                            <th className="px-3 py-2 font-bold text-xs uppercase tracking-wide text-muted-foreground">
                                              Date
                                            </th>
                                            <th className="px-3 py-2 font-bold text-xs uppercase tracking-wide text-muted-foreground text-right">
                                              Amount
                                            </th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {breakdown.map((b, j) => (
                                            <tr key={`${b.insider_name}-${b.trans_date}-${j}`} className="border-b border-border/80">
                                              <td className="px-3 py-2.5 font-medium text-foreground">{b.insider_name || "—"}</td>
                                              <td className="px-3 py-2.5 text-muted-foreground">{b.role || "—"}</td>
                                              <td className="px-3 py-2.5 tabular-nums text-muted-foreground">
                                                {fmtClusterDate(b.trans_date)}
                                              </td>
                                              <td className="px-3 py-2.5 text-right font-semibold tabular-nums">
                                                {formatUsdSmart(b.amount_usd)}
                                              </td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                    </div>
                                  )}
                                </div>
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </main>

      {selectedChart && (
        <CandlestickModal
          ticker={selectedChart.ticker}
          transDate={selectedChart.transDate}
          onClose={() => setSelectedChart(null)}
        />
      )}
    </div>
  );
}
