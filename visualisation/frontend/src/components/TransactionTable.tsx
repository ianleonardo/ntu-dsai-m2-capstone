"use client";

import React, { useMemo, useState } from "react";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Info,
} from "lucide-react";
import { cn } from "@/lib/utils";
import CandlestickModal from "./CandlestickModal";
import {
  type TxRow,
  pick,
  strVal,
  transactionUsd,
  totalReportedShares,
  transactionTypeLabel,
  pricePerShare,
  sharesHeld,
  txnSizeBadge,
  formatUsdSmart,
  formatUsdPerShare,
  formatShares,
  parseRowDate,
  fmtDetailTableDate,
  filingLagDays,
  firstOwnerName,
  rolePills,
  sectorChip,
  lookupTickerClose,
  returnVsTxnPricePct,
  formatReturnPctDisplay,
  returnPctColorClass,
} from "@/lib/transactionRowModel";

interface TransactionTableProps {
  rowData: Record<string, unknown>[];
  /** Uppercase ticker → latest daily close (from cached S&P directory). */
  tickerClose?: ReadonlyMap<string, number> | null;
}

type SortKey =
  | "ticker"
  | "insider"
  | "role"
  | "transDate"
  | "filingDate"
  | "txnType"
  | "value"
  | "shares";

function Th({
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
  sortKey: SortKey;
  activeKey: SortKey;
  dir: "asc" | "desc";
  onSort: (k: SortKey) => void;
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

function sortValue(row: TxRow, key: SortKey): string | number {
  switch (key) {
    case "ticker":
      return strVal(pick(row, ["ISSUERTRADINGSYMBOL"])).toUpperCase();
    case "insider":
      return firstOwnerName(row).toLowerCase();
    case "role":
      return rolePills(row).join(" ").toLowerCase();
    case "transDate": {
      const d = parseRowDate(pick(row, ["TRANS_DATE", "trans_date"]));
      return d ? d.getTime() : 0;
    }
    case "filingDate": {
      const d = parseRowDate(pick(row, ["FILING_DATE", "filing_date"]));
      return d ? d.getTime() : 0;
    }
    case "txnType":
      return transactionTypeLabel(row).toLowerCase();
    case "value":
      return transactionUsd(row);
    case "shares":
      return totalReportedShares(row);
    default:
      return 0;
  }
}

/** Widths sum to 100%; table-fixed keeps all columns on screen without horizontal scroll. */
const COL_PCT = [13, 12, 11, 12, 7, 14, 16, 15] as const;

export default function TransactionTable({ rowData, tickerClose = null }: TransactionTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("transDate");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [selectedChart, setSelectedChart] = useState<{ ticker: string; transDate: string } | null>(null);

  const onSort = (k: SortKey) => {
    if (k === sortKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortKey(k);
      setSortDir(
        k === "ticker" ||
        k === "insider" ||
        k === "role" ||
        k === "transDate" ||
        k === "txnType"
          ? "asc"
          : "desc"
      );
    }
  };

  const sorted = useMemo(() => {
    const rows = [...rowData] as TxRow[];
    rows.sort((a, b) => {
      const va = sortValue(a, sortKey);
      const vb = sortValue(b, sortKey);
      const mul = sortDir === "asc" ? 1 : -1;
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * mul;
      return String(va).localeCompare(String(vb)) * mul;
    });
    return rows;
  }, [rowData, sortKey, sortDir]);

  return (
    <div className="w-full rounded-xl border border-border shadow-xl bg-card">
      <table className="w-full table-fixed text-[11px] leading-snug border-collapse">
        <colgroup>
          {COL_PCT.map((w, i) => (
            <col key={i} style={{ width: `${w}%` }} />
          ))}
        </colgroup>
        <thead>
          <tr className="border-b border-border bg-muted/40">
            <Th
              label="Ticker"
              info="Symbol, company, transaction-size tier (by $)."
              sortKey="ticker"
              activeKey={sortKey}
              dir={sortDir}
              onSort={onSort}
              className="pl-2"
            />
            <Th label="Insider" sortKey="insider" activeKey={sortKey} dir={sortDir} onSort={onSort} />
            <Th label="Role" sortKey="role" activeKey={sortKey} dir={sortDir} onSort={onSort} />
            <Th label="Dates" sortKey="transDate" activeKey={sortKey} dir={sortDir} onSort={onSort} />
            <Th
              label="Type"
              info="SEC transaction coding label from the filing when available."
              sortKey="txnType"
              activeKey={sortKey}
              dir={sortDir}
              onSort={onSort}
            />
            <Th
              label="Value / Price"
              info="Filing notional from the mart: fact dollars or est_acquire + est_dispose; only P/S (Purchase/Sale) lines, so implied price matches open-market trades."
              sortKey="value"
              activeKey={sortKey}
              dir={sortDir}
              onSort={onSort}
              className="text-right [&_button]:w-full [&_button]:justify-end"
            />
            <Th label="Shares / Own" sortKey="shares" activeKey={sortKey} dir={sortDir} onSort={onSort} className="text-right [&_button]:w-full [&_button]:justify-end" />
            <th className="px-1 py-2 text-right align-bottom font-bold text-xs uppercase tracking-wide text-muted-foreground pr-2">
              <span
                className="inline-flex items-center justify-end gap-0.5 w-full leading-tight"
                title="Return = (latest close − transaction price) ÷ transaction price. Close from sp500_stock_daily (max date)."
              >
                Ret / Curr.
                <Info className="w-3 h-3 opacity-60 shrink-0" aria-hidden />
              </span>
            </th>
          </tr>
        </thead>
        <tbody>
          {sorted.length === 0 ? (
            <tr>
              <td colSpan={8} className="px-2 py-12 text-center text-muted-foreground">
                No transactions for the current filters.
              </td>
            </tr>
          ) : (
            sorted.map((row, i) => {
              const ticker = strVal(pick(row, ["ISSUERTRADINGSYMBOL"]));
              const symKey = strVal(
                pick(row, ["symbol_norm", "SYMBOL_NORM", "ISSUERTRADINGSYMBOL"])
              ).toUpperCase();
              const company = strVal(pick(row, ["ISSUERNAME"]));
              const usd = transactionUsd(row);
              const shares = totalReportedShares(row);
              const txnType = transactionTypeLabel(row);
              const held = sharesHeld(row);
              const pps = pricePerShare(row);
              const lastClose = lookupTickerClose(tickerClose, symKey);
              const retPct = returnVsTxnPricePct(lastClose, pps);
              const transD = parseRowDate(pick(row, ["TRANS_DATE", "trans_date"]));
              const filingD = parseRowDate(pick(row, ["FILING_DATE", "filing_date"]));
              const lag = filingLagDays(filingD, transD);
              const sizeLabel = usd > 0 ? txnSizeBadge(usd) : "";
              const sector = sectorChip(row);
              const roles = rolePills(row);
              const showBadges = Boolean(sector);

              return (
                <tr
                  key={`${strVal(pick(row, ["ACCESSION_NUMBER"]))}-${i}`}
                  className="border-b border-border/70 hover:bg-muted/20 transition-colors"
                >
                  <td className="px-1 py-2 pl-2 align-top overflow-hidden">
                    <button
                      type="button"
                      onClick={() => {
                        if (ticker && transD) {
                          setSelectedChart({ ticker, transDate: transD.toISOString() });
                        }
                      }}
                      className="font-black text-foreground tracking-tight truncate hover:text-primary hover:underline transition-colors text-left"
                      title={ticker ? `View ${ticker} chart` : undefined}
                    >
                      {ticker || "\u00a0"}
                    </button>
                    {company ? (
                      <div className="text-[10px] text-muted-foreground truncate mt-0.5" title={company}>
                        {company}
                      </div>
                    ) : null}
                    {showBadges ? (
                      <div className="flex flex-wrap gap-0.5 mt-1">
                        {sector ? (
                          <span className="text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded-full border border-primary/30 text-primary/90 truncate max-w-full">
                            {sector}
                          </span>
                        ) : null}
                      </div>
                    ) : null}
                  </td>
                  <td className="px-1 py-2 align-top overflow-hidden">
                    <span className="text-blue-400 font-medium line-clamp-2 break-words">{firstOwnerName(row)}</span>
                  </td>
                  <td className="px-1 py-2 align-top overflow-hidden">
                    <div className="flex flex-wrap gap-0.5">
                      {roles.length === 0 ? (
                        <span className="text-muted-foreground text-[10px]"> </span>
                      ) : (
                        roles.map((r) => (
                          <span
                            key={r}
                            className="text-[9px] font-semibold px-1 py-0.5 rounded-full bg-muted text-muted-foreground border border-border/80 truncate max-w-full"
                          >
                            {r}
                          </span>
                        ))
                      )}
                    </div>
                  </td>
                  <td className="px-1 py-2 align-top">
                    <div className="font-bold text-foreground whitespace-nowrap">{fmtDetailTableDate(transD)}</div>
                    <div className="text-[10px] text-muted-foreground mt-0.5">
                      Filed {fmtDetailTableDate(filingD)}
                    </div>
                    {lag != null ? (
                      <div
                        className={cn(
                          "text-[10px] mt-0.5 font-medium whitespace-nowrap",
                          lag > 0 ? "text-rose-400/90" : "text-muted-foreground"
                        )}
                      >
                        ({lag >= 0 ? "+" : ""}
                        {lag}d)
                      </div>
                    ) : null}
                  </td>
                  <td className="px-1 py-2 align-top overflow-hidden">
                    <span
                      className="inline-flex text-[9px] font-semibold px-1.5 py-0.5 rounded-md border border-border bg-muted/50 text-muted-foreground max-w-full truncate"
                      title={txnType !== "—" ? txnType : undefined}
                    >
                      {txnType}
                    </span>
                  </td>
                  <td className="px-1 py-2 align-top text-right overflow-hidden">
                    <div className="font-bold text-foreground truncate" title={formatUsdSmart(usd)}>
                      {formatUsdSmart(usd)}
                    </div>
                    {sizeLabel ? (
                      <div className="mt-0.5 flex justify-end">
                        <span className="text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded-full border border-border bg-secondary/60 text-muted-foreground">
                          {sizeLabel}
                        </span>
                      </div>
                    ) : null}
                    <div className="text-[10px] text-muted-foreground mt-0.5 truncate">
                      {pps != null ? formatUsdPerShare(pps) : "—"}
                    </div>
                  </td>
                  <td className="px-1 py-2 align-top text-right overflow-hidden">
                    <div className="font-semibold text-foreground break-all">{formatShares(shares)}</div>
                    {held > 0 ? (
                      <div className="text-[10px] text-muted-foreground mt-0.5 truncate">
                        Held {formatShares(held)}
                      </div>
                    ) : null}
                  </td>
                  <td className="px-1 py-2 pr-2 align-top text-right overflow-hidden">
                    <div className={cn("tabular-nums", returnPctColorClass(retPct))}>
                      {formatReturnPctDisplay(retPct)}
                    </div>
                    <div className="text-[10px] text-muted-foreground mt-0.5 truncate">
                      Curr. {lastClose != null ? formatUsdPerShare(lastClose) : "—"}
                    </div>
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
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
