"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Search, Plus, UserRoundSearch, X } from "lucide-react";
import {
  loadSearchDirectoryCache,
  ensureSearchDirectoryLoaded,
  type StockRow,
  type InsiderRow,
} from "@/lib/searchDirectoryCache";
import {
  parseSearchValueToChips,
  addStockPick,
  addInsiderPick,
  removeChipAt,
  commitDraftToken,
  type SearchChip,
} from "@/lib/searchChips";
import { cn } from "@/lib/utils";

export type { StockRow, InsiderRow };

interface DirectorySearchBarProps {
  value: string;
  onChange: (q: string) => void;
  placeholder?: string;
  className?: string;
}

function draftFilterTokens(draft: string): string[] {
  return draft
    .split(/[,;\n]+/)
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

function chipPillClass(kind: SearchChip["kind"]): string {
  if (kind === "ticker") {
    return "bg-sky-200/90 text-sky-950 dark:bg-sky-900/50 dark:text-sky-100 border border-sky-400/40 dark:border-sky-600/40";
  }
  if (kind === "insider") {
    return "bg-violet-200/90 text-violet-950 dark:bg-violet-900/45 dark:text-violet-100 border border-violet-400/40 dark:border-violet-600/40";
  }
  return "bg-muted text-foreground border border-border";
}

export default function DirectorySearchBar({
  value,
  onChange,
  placeholder = "Type to search…",
  className,
}: DirectorySearchBarProps) {
  const [stocks, setStocks] = useState<StockRow[]>([]);
  const [insiders, setInsiders] = useState<InsiderRow[]>([]);
  const [open, setOpen] = useState(false);
  const [focused, setFocused] = useState(false);
  const [dirLoading, setDirLoading] = useState(false);
  const [draft, setDraft] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const blurCloseTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearBlurCloseTimer = useCallback(() => {
    if (blurCloseTimerRef.current != null) {
      clearTimeout(blurCloseTimerRef.current);
      blurCloseTimerRef.current = null;
    }
  }, []);

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
        setDirLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => () => clearBlurCloseTimer(), [clearBlurCloseTimer]);

  const chips = useMemo(
    () => parseSearchValueToChips(value, stocks, insiders),
    [value, stocks, insiders]
  );

  const filterTokens = useMemo(() => draftFilterTokens(draft), [draft]);

  const { stockHits, insiderHits } = useMemo(() => {
    if (filterTokens.length === 0) {
      return {
        stockHits: stocks.slice(0, 8),
        insiderHits: insiders.slice(0, 8),
      };
    }
    const stockHits = stocks
      .filter((r) =>
        filterTokens.some(
          (q) =>
            r.ticker.includes(q) ||
            r.company.toUpperCase().includes(q) ||
            (r.sector && r.sector.toUpperCase().includes(q))
        )
      )
      .slice(0, 25);
    const insiderHits = insiders
      .filter((r) => {
        const blob = [r.name, r.role_type, r.title, r.cik].filter(Boolean).join(" ").toUpperCase();
        return filterTokens.some((q) => blob.includes(q));
      })
      .slice(0, 25);
    return { stockHits, insiderHits };
  }, [stocks, insiders, filterTokens]);

  const directoryEmpty = !dirLoading && stocks.length === 0 && insiders.length === 0;

  const hasActiveFilter = draft.trim().length > 0 || value.trim().length > 0;
  const showPanel =
    open &&
    focused &&
    (dirLoading ||
      stockHits.length > 0 ||
      insiderHits.length > 0 ||
      hasActiveFilter ||
      directoryEmpty);

  const focusInput = useCallback(() => inputRef.current?.focus(), []);

  const pickStock = useCallback(
    (r: StockRow) => {
      onChange(addStockPick(value, stocks, insiders, r));
      setDraft("");
      clearBlurCloseTimer();
      focusInput();
      setOpen(true);
      setFocused(true);
    },
    [value, stocks, insiders, onChange, focusInput, clearBlurCloseTimer]
  );

  const pickInsider = useCallback(
    (r: InsiderRow) => {
      onChange(addInsiderPick(value, stocks, insiders, r));
      setDraft("");
      clearBlurCloseTimer();
      focusInput();
      setOpen(true);
      setFocused(true);
    },
    [value, stocks, insiders, onChange, focusInput, clearBlurCloseTimer]
  );

  const removeAt = useCallback(
    (index: number) => {
      onChange(removeChipAt(value, stocks, insiders, index));
      clearBlurCloseTimer();
      focusInput();
      setOpen(true);
      setFocused(true);
    },
    [value, stocks, insiders, onChange, focusInput, clearBlurCloseTimer]
  );

  const commitDraft = useCallback(() => {
    if (!draft.trim()) return;
    const next = commitDraftToken(value, stocks, insiders, draft);
    if (next !== value) onChange(next);
    setDraft("");
  }, [value, stocks, insiders, draft, onChange]);

  return (
    <div className={cn("relative min-w-[min(100%,28rem)] max-w-xl flex-1", className)}>
      <div
        role="search"
        className={cn(
          "flex flex-wrap items-center gap-1.5 rounded-xl border bg-card pl-2.5 pr-2 py-1.5 transition-colors cursor-text min-h-[42px]",
          focused ? "border-primary ring-1 ring-primary/30" : "border-border"
        )}
      >
        <Search className="w-4 h-4 text-primary shrink-0 ml-0.5" aria-hidden />
        <div
          className="flex flex-wrap items-center gap-1.5 flex-1 min-w-[6rem]"
          data-chip-row
          onMouseDown={(e) => {
            const el = e.target as HTMLElement;
            if (el.tagName === "INPUT" || el.closest("button")) return;
            e.preventDefault();
            focusInput();
          }}
        >
          {chips.map((chip, index) => (
            <span
              key={chip.key}
              className={cn(
                "inline-flex items-center gap-0.5 max-w-[min(100%,14rem)] rounded-full pl-2.5 pr-1 py-0.5 text-xs font-medium",
                chipPillClass(chip.kind)
              )}
            >
              <span className="truncate">{chip.label}</span>
              <button
                type="button"
                className="shrink-0 rounded-full p-0.5 hover:bg-black/10 dark:hover:bg-white/10 outline-none focus-visible:ring-2 focus-visible:ring-primary/50"
                aria-label={`Remove ${chip.label}`}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => removeAt(index)}
              >
                <X className="w-3.5 h-3.5 opacity-70" aria-hidden />
              </button>
            </span>
          ))}
          <input
            ref={inputRef}
            type="search"
            placeholder={chips.length === 0 ? placeholder : "Add filter…"}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                commitDraft();
              } else if (e.key === "Backspace" && draft === "" && chips.length > 0) {
                e.preventDefault();
                removeAt(chips.length - 1);
              } else if (e.key === "Escape") {
                setOpen(false);
                inputRef.current?.blur();
              }
            }}
            onFocus={() => {
              clearBlurCloseTimer();
              setOpen(true);
              setFocused(true);
            }}
            onBlur={() => {
              setFocused(false);
              clearBlurCloseTimer();
              blurCloseTimerRef.current = setTimeout(() => {
                blurCloseTimerRef.current = null;
                setOpen(false);
              }, 180);
            }}
            autoComplete="off"
            className="flex-1 min-w-[7rem] bg-transparent text-sm outline-none py-1.5 px-0.5"
          />
        </div>
      </div>

      {showPanel && (
        <div className="absolute z-50 mt-2 w-full max-h-80 overflow-y-auto rounded-xl border border-border bg-card shadow-2xl text-sm">
          {dirLoading && (
            <p className="px-3 py-4 text-center text-xs text-muted-foreground">Loading directory…</p>
          )}
          {directoryEmpty && !hasActiveFilter && (
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
                      onClick={() => pickStock(r)}
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
                      onClick={() => pickInsider(r)}
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

          {!dirLoading && filterTokens.length > 0 && stockHits.length === 0 && insiderHits.length === 0 && (
            <p className="px-3 py-6 text-center text-muted-foreground text-xs">No matches in directory.</p>
          )}
        </div>
      )}
    </div>
  );
}
