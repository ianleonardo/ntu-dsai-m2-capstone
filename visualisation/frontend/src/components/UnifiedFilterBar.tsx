import React from "react";
import DirectorySearchBar from "./DirectorySearchBar";
import DateRangePicker from "./DateRangePicker";
import MultiSelectDropdown from "./MultiSelectDropdown";
import GroupedMultiSelectDropdown from "./GroupedMultiSelectDropdown";
import { useUnifiedFilters, SECTORS, SizeTier, ROLE_GROUPS } from "@/lib/unifiedFiltersStorage";
import { cn } from "@/lib/utils";

interface UnifiedFilterBarProps {
  dateRange: { start: string; end: string };
  onDateRangeChange: (r: { start: string; end: string }) => void;
  onApply: () => void;
  loading: boolean;
}

export default function UnifiedFilterBar({
  dateRange,
  onDateRangeChange,
  onApply,
  loading = false,
}: UnifiedFilterBarProps) {
  const { filters, updateFilters } = useUnifiedFilters();

  const SIZES_LABELS = [
    { value: "All", label: "All" },
    { value: "Mega", label: "Mega (>$100M)" },
    { value: "Large", label: "Large ($10M - $100M)" },
    { value: "Mid", label: "Mid ($1M - $10M)" },
    { value: "Small", label: "Small ($100K - $1M)" },
    { value: "Micro", label: "Micro (<$100K)" },
  ];

  return (
    <div className="flex flex-col gap-3 w-full lg:w-auto lg:min-w-[40rem]">
      {/* Top Row: Search & Date */}
      <div className="flex flex-col sm:flex-row flex-wrap items-stretch sm:items-center gap-3">
        <DirectorySearchBar
          value={filters.search}
          onChange={(v) => updateFilters({ search: v })}
          className="w-full flex-1 min-w-[16rem]"
        />
        <DateRangePicker
          value={dateRange}
          onChange={onDateRangeChange}
        />
      </div>

      {/* Bottom Row: Unified Dropdowns + Apply Button */}
      <div className="flex flex-wrap items-stretch gap-3">
        <MultiSelectDropdown
          label="Sector"
          options={SECTORS as unknown as string[]}
          selected={filters.sector}
          onChange={(selected) => updateFilters({ sector: selected })}
          className="flex-1 sm:flex-none"
        />

        <div className="flex flex-col items-start gap-0.5 border border-border rounded-lg bg-card px-3 py-1.5 focus-within:ring-2 focus-within:ring-primary/50 flex-1 sm:flex-none">
          <label className="text-[10px] uppercase font-bold text-muted-foreground leading-none">Size</label>
          <select
            className="text-sm font-medium bg-transparent text-foreground outline-none cursor-pointer w-full appearance-none pr-4"
            value={filters.size}
            onChange={(e) => updateFilters({ size: e.target.value as SizeTier })}
          >
            {SIZES_LABELS.map((s) => (
              <option key={s.value} value={s.value}>{s.label}</option>
            ))}
          </select>
        </div>

        <GroupedMultiSelectDropdown
          label="Role"
          groups={ROLE_GROUPS}
          selected={filters.role}
          onChange={(selected) => updateFilters({ role: selected })}
          className="flex-1 sm:flex-none"
        />

        <button
          type="button"
          onClick={onApply}
          disabled={loading}
          className={cn(
            "shrink-0 px-8 py-2.5 rounded-xl text-sm font-bold shadow transition-colors ml-auto",
            "bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:pointer-events-none"
          )}
        >
          {loading ? "Loading…" : "Apply"}
        </button>
      </div>
    </div>
  );
}
