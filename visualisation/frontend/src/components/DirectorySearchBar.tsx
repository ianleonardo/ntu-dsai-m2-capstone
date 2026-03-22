"use client";

import React, { useEffect, useMemo, useState } from "react";
import { Search, Plus, UserRoundSearch } from "lucide-react";
import {
  loadSearchDirectoryCache,
  ensureSearchDirectoryLoaded,
  type StockRow,
  type InsiderRow,
} from "@/lib/searchDirectoryCache";
import { cn } from "@/lib/utils";

export type { StockRow, InsiderRow };

interface DirectorySearchBarProps {
  value: string;
  onChange: (q: string) => void;
  placeholder?: string;
  className?: string;
}

export default function DirectorySearchBar({
  value,
  onChange,
  placeholder = "Search ticker, company, or insider…",
  className,
}: DirectorySearchBarProps) {
  const [stocks, setStocks] = useState<StockRow[]>([]);
  const [insiders, setInsiders] = useState<InsiderRow[]>([]);
  const [open, setOpen] = useState(false);
  const [focused, setFocused] = useState(false);
  const [dirLoading, setDirLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setDirLoading(true);
      try {
        await ensureSearchDirectoryLoaded();
        if (cancelled) return;
        const hit = loadSearchDirectoryCache();
        if (hit) {
          setStocks(hit.stocks);
          setInsiders(hit.insiders);
        }
      } catch (e) {
        console.error("search-directory:", e);
      } finally {
        // Always clear spinner (Strict Mode unmount can cancel state updates otherwise).
        setDirLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const q = value.trim().toUpperCase();
  const { stockHits, insiderHits } = useMemo(() => {
    if (!q) {
      return {
        stockHits: stocks.slice(0, 8),
        insiderHits: insiders.slice(0, 8),
      };
    }
    const stockHits = stocks
      .filter(
        (r) =>
          r.ticker.includes(q) ||
          r.company.toUpperCase().includes(q) ||
          (r.sector && r.sector.toUpperCase().includes(q))
      )
      .slice(0, 25);
    const insiderHits = insiders
      .filter((r) => {
        const blob = [r.name, r.role_type, r.title, r.cik].filter(Boolean).join(" ").toUpperCase();
        return blob.includes(q);
      })
      .slice(0, 25);
    return { stockHits, insiderHits };
  }, [stocks, insiders, q]);

  const directoryEmpty = !dirLoading && stocks.length === 0 && insiders.length === 0;

  const showPanel =
    open &&
    focused &&
    (dirLoading ||
      stockHits.length > 0 ||
      insiderHits.length > 0 ||
      q.length > 0 ||
      directoryEmpty);

  return (
    <div className={cn("relative min-w-[min(100%,28rem)] max-w-xl flex-1", className)}>
      <div
        className={cn(
          "flex items-center gap-2 rounded-xl border bg-card pl-3 pr-2 py-2 transition-colors",
          focused ? "border-primary ring-1 ring-primary/30" : "border-border"
        )}
      >
        <Search className="w-4 h-4 text-muted-foreground shrink-0" />
        <input
          type="search"
          placeholder={placeholder}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onFocus={() => {
            setOpen(true);
            setFocused(true);
          }}
          onBlur={() => {
            setFocused(false);
            setTimeout(() => setOpen(false), 180);
          }}
          autoComplete="off"
          className="flex-1 bg-transparent text-sm outline-none min-w-0 py-1"
        />
      </div>

      {showPanel && (
        <div className="absolute z-50 mt-2 w-full max-h-80 overflow-y-auto rounded-xl border border-border bg-card shadow-2xl text-sm">
          {dirLoading && (
            <p className="px-3 py-4 text-center text-xs text-muted-foreground">Loading directory…</p>
          )}
          {directoryEmpty && q.length === 0 && (
            <p className="px-3 py-4 text-center text-xs text-muted-foreground">
              Directory could not be loaded. In dev, ensure the FastAPI server is running on port 8000 (Next proxies
              via <code className="text-[10px] bg-muted px-1 rounded">/insider-api</code>). Check the browser console;
              override with <code className="text-[10px] bg-muted px-1 rounded">NEXT_PUBLIC_API_URL</code> if needed.
            </p>
          )}
          {!dirLoading && stockHits.length > 0 && (
            <div className="border-b border-border pb-1">
              <p className="px-3 pt-2 pb-1 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                Markets (stocks)
              </p>
              <ul>
                {stockHits.map((r) => (
                  <li key={r.ticker} className="border-t border-border/60 first:border-t-0">
                    <button
                      type="button"
                      className="flex w-full items-center gap-3 px-3 py-2.5 text-left hover:bg-muted/80"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => {
                        onChange(r.ticker);
                        setOpen(false);
                      }}
                    >
                      <span className="font-bold text-foreground w-14 shrink-0">{r.ticker}</span>
                      <span className="rounded-md bg-muted px-2 py-0.5 text-[10px] font-semibold uppercase text-muted-foreground shrink-0">
                        Stock
                      </span>
                      <span className="flex-1 truncate text-muted-foreground">{r.company}</span>
                      <Plus className="w-4 h-4 shrink-0 text-muted-foreground" aria-hidden />
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {!dirLoading && insiderHits.length > 0 && (
            <div>
              <p className="px-3 pt-2 pb-1 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                Insiders
              </p>
              <ul>
                {insiderHits.map((r, i) => (
                  <li
                    key={`insider-${i}-${r.cik ?? ""}-${r.name}-${r.role_type ?? ""}-${r.title ?? ""}`}
                    className="border-t border-border/60 first:border-t-0"
                  >
                    <button
                      type="button"
                      className="flex w-full items-center gap-3 px-3 py-2.5 text-left hover:bg-muted/80"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => {
                        onChange(r.name);
                        setOpen(false);
                      }}
                    >
                      <span className="rounded-md bg-violet-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase text-violet-300 border border-violet-500/25 shrink-0">
                        Insider
                      </span>
                      <span className="flex-1 min-w-0">
                        <span className="font-medium text-foreground block truncate">{r.name}</span>
                        {(r.title || r.role_type) && (
                          <span className="text-xs text-muted-foreground block truncate">
                            {[r.title, r.role_type].filter(Boolean).join(" · ")}
                          </span>
                        )}
                      </span>
                      <UserRoundSearch className="w-4 h-4 text-muted-foreground shrink-0" />
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {!dirLoading && q.length > 0 && stockHits.length === 0 && insiderHits.length === 0 && (
            <p className="px-3 py-6 text-center text-muted-foreground text-xs">No matches in directory.</p>
          )}
        </div>
      )}
    </div>
  );
}
