"use client";

import React, { useState, useRef, useEffect, useMemo } from "react";
import { Calendar, ChevronDown, ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { defaultDateRangeMonths, defaultDateRangeDays } from "@/lib/dateRangeStorage";

interface DateRange {
  start: string;
  end: string;
}

interface DateRangePickerProps {
  value: DateRange;
  onChange: (range: DateRange) => void;
}

const PRESETS = [
  { label: "Past 24 Hours", days: 1 },
  { label: "Past 3 Days",   days: 3 },
  { label: "Past 1 Week",   days: 7 },
  { label: "Past 1 Month",  months: 1 },
  { label: "Past 3 Months", months: 3 },
  { label: "Past 6 Months", months: 6 },
  { label: "Past 1 Year",   months: 12 },
  { label: "Past 3 Years",  months: 36 },
  { label: "Custom",        custom: true },
] as const;

function fmt(d: Date) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

type RangedPreset = Extract<(typeof PRESETS)[number], { days: number } | { months: number }>;

/** Same UTC calendar rules as `defaultStoredDateRange()` so first visit shows "Past 6 Months", not Custom. */
function presetRange(preset: RangedPreset): DateRange {
  if ("days" in preset) return defaultDateRangeDays(preset.days);
  return defaultDateRangeMonths(preset.months);
}

const MONTHS = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"];
const DAYS   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function sameDay(a: Date, b: Date) {
  return a.getFullYear() === b.getFullYear() &&
         a.getMonth() === b.getMonth() &&
         a.getDate() === b.getDate();
}

function rangesClose(a: { start: string; end: string }, b: { start: string; end: string }) {
  return a.start === b.start && a.end === b.end;
}

interface MiniCalProps {
  year: number;
  month: number; // 0-indexed
  rangeStart: Date | null;
  rangeEnd:   Date | null;
  hovered:    Date | null;
  onDayClick: (d: Date) => void;
  onDayHover: (d: Date) => void;
}

function MiniCal({ year, month, rangeStart, rangeEnd, hovered, onDayClick, onDayHover }: MiniCalProps) {
  // Build grid — offset to start week on Monday
  const firstDay = new Date(year, month, 1);
  const startOffset = (firstDay.getDay() + 6) % 7; // Monday = 0
  const daysInMonth = new Date(year, month + 1, 0).getDate();

  const effectiveEnd = rangeStart && hovered && !rangeEnd ? hovered : rangeEnd;

  const cells: (Date | null)[] = [
    ...Array(startOffset).fill(null),
    ...Array.from({ length: daysInMonth }, (_, i) => new Date(year, month, i + 1)),
  ];
  // pad to full rows
  while (cells.length % 7) cells.push(null);

  return (
    <div className="min-w-[240px]">
      <p className="text-center font-bold text-sm mb-4">
        {MONTHS[month]} {year}
      </p>
      <div className="grid grid-cols-7 text-[11px] text-muted-foreground mb-2">
        {DAYS.map(d => <span key={d} className="text-center pb-1">{d}</span>)}
      </div>
      <div className="grid grid-cols-7 gap-y-1">
        {cells.map((d, i) => {
          if (!d) return <span key={`e-${i}`} />;
          const isStart    = rangeStart && sameDay(d, rangeStart);
          const isEnd      = effectiveEnd && sameDay(d, effectiveEnd);
          const inRange    = rangeStart && effectiveEnd && d > rangeStart && d < effectiveEnd;
          const isSingle   = isStart && isEnd;

          return (
            <button
              key={d.toISOString()}
              onMouseEnter={() => onDayHover(d)}
              onClick={() => onDayClick(d)}
              className={cn(
                "text-xs h-8 w-full text-center transition-colors rounded-lg font-medium",
                isSingle && "bg-primary text-primary-foreground rounded-lg",
                !isSingle && isStart && "bg-primary text-primary-foreground rounded-l-lg rounded-r-none",
                !isSingle && isEnd   && "bg-primary text-primary-foreground rounded-r-lg rounded-l-none",
                inRange && "bg-primary/20 text-foreground rounded-none",
                !isStart && !isEnd && !inRange && "hover:bg-muted"
              )}
            >
              {d.getDate()}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export default function DateRangePicker({ value, onChange }: DateRangePickerProps) {
  const [open, setOpen] = useState(false);
  const [showCustom, setShowCustom] = useState(false);

  // Calendar state
  const today = new Date();
  const [viewYear,  setViewYear]  = useState(today.getFullYear());
  const [viewMonth, setViewMonth] = useState(today.getMonth());

  const [picking,    setPicking]    = useState<"start" | "end">("start");
  const [tempStart,  setTempStart]  = useState<Date | null>(null);
  const [tempEnd,    setTempEnd]    = useState<Date | null>(null);
  const [hovered,    setHovered]    = useState<Date | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    function handler(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const matchedPresetLabel = useMemo(() => {
    const found = PRESETS.find((p) => {
      if ("custom" in p && p.custom) return false;
      return rangesClose(presetRange(p as RangedPreset), value);
    });
    return found?.label ?? "Custom";
  }, [value]);

  const triggerLabel = showCustom ? "Custom" : matchedPresetLabel;

  function presetSelected(p: (typeof PRESETS)[number]) {
    if ("custom" in p && p.custom) return showCustom;
    return !showCustom && matchedPresetLabel === p.label;
  }

  function selectPreset(p: (typeof PRESETS)[number]) {
    if ("custom" in p && p.custom) {
      setShowCustom(true);
      return;
    }
    setShowCustom(false);
    onChange(presetRange(p as RangedPreset));
    setOpen(false);
  }

  // Calendar navigation — show left & right month
  const leftYear  = viewMonth === 0 ? viewYear - 1 : viewYear;
  const leftMonth = viewMonth === 0 ? 11 : viewMonth - 1;

  function prevMonth() {
    if (viewMonth === 0) { setViewMonth(11); setViewYear(y => y - 1); }
    else setViewMonth(m => m - 1);
  }
  function nextMonth() {
    if (viewMonth === 11) { setViewMonth(0); setViewYear(y => y + 1); }
    else setViewMonth(m => m + 1);
  }

  function handleDayClick(d: Date) {
    if (picking === "start") {
      setTempStart(d);
      setTempEnd(null);
      setPicking("end");
    } else {
      if (tempStart && d < tempStart) {
        setTempEnd(tempStart);
        setTempStart(d);
      } else {
        setTempEnd(d);
      }
      setPicking("start");
      // Apply immediately
      const s = tempStart && d >= tempStart ? tempStart : d;
      const e = tempStart && d >= tempStart ? d : tempStart!;
      onChange({ start: fmt(s!), end: fmt(e) });
      setShowCustom(false);
      setOpen(false);
    }
  }

  return (
    <div ref={containerRef} className="relative">
      {/* Trigger */}
      <button
        onClick={() => setOpen(o => !o)}
        className={cn(
          "flex items-center gap-2 px-4 py-2.5 rounded-xl border text-sm font-semibold transition-all",
          open
            ? "border-primary bg-primary/10 text-primary"
            : "border-border bg-card text-foreground hover:border-primary/50"
        )}
      >
        <Calendar className="w-4 h-4" />
        {triggerLabel}
        <ChevronDown className={cn("w-4 h-4 transition-transform", open && "rotate-180")} />
      </button>

      {/* Dropdown panel */}
      {open && (
        <div className="absolute z-50 top-full mt-2 right-0 flex shadow-2xl rounded-2xl border border-border bg-card overflow-hidden">
          {/* Left — preset list */}
          <div className="w-44 border-r border-border p-3 flex flex-col gap-0.5">
            {PRESETS.map(p => (
              <button
                key={p.label}
                onClick={() => selectPreset(p)}
                className={cn(
                  "text-left px-3 py-2 rounded-lg text-sm transition-colors",
                  presetSelected(p)
                    ? "font-bold text-foreground bg-secondary"
                    : "text-muted-foreground hover:bg-muted hover:text-foreground"
                )}
              >
                {p.label}
              </button>
            ))}
          </div>

          {/* Right — dual-month calendar (shown for Custom) */}
          {showCustom && (
            <div className="p-5">
              {/* Navigation */}
              <div className="flex justify-between items-center mb-5">
                <button onClick={prevMonth} className="p-1 rounded-lg hover:bg-muted transition-colors">
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <button onClick={nextMonth} className="p-1 rounded-lg hover:bg-muted transition-colors">
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
              <div className="flex gap-8">
                <MiniCal
                  year={leftYear} month={leftMonth}
                  rangeStart={tempStart} rangeEnd={tempEnd} hovered={hovered}
                  onDayClick={handleDayClick} onDayHover={setHovered}
                />
                <MiniCal
                  year={viewYear} month={viewMonth}
                  rangeStart={tempStart} rangeEnd={tempEnd} hovered={hovered}
                  onDayClick={handleDayClick} onDayHover={setHovered}
                />
              </div>
              <p className="text-xs text-muted-foreground mt-4 text-center">
                {picking === "start" ? "Click to set start date" : "Click to set end date"}
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
