"use client";

import React, { useState, useRef, useEffect } from "react";
import { ChevronDown, Check } from "lucide-react";

interface Props {
  label: string;
  options: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  className?: string;
}

export default function MultiSelectDropdown({ label, options, selected, onChange, className = "" }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (ref.current && !ref.current.contains(event.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const toggleOption = (opt: string) => {
    if (selected.includes(opt)) {
      onChange(selected.filter((o) => o !== opt));
    } else {
      onChange([...selected, opt]);
    }
  };

  const displayText = selected.length === 0 ? "All" : selected.length === 1 ? selected[0] : `${selected.length} Selected`;

  return (
    <div className={`relative ${className}`} ref={ref}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex items-center justify-between w-full h-full min-w-[9rem] border border-border rounded-lg bg-card px-3 py-2 text-sm font-medium text-foreground outline-none focus:ring-2 focus:ring-primary/50"
      >
        <div className="flex flex-col items-start gap-0.5 pointer-events-none">
          <span className="text-[10px] uppercase font-bold text-muted-foreground leading-none">{label}</span>
          <span className="leading-none text-left line-clamp-1">{displayText}</span>
        </div>
        <ChevronDown className="shrink-0 w-4 h-4 text-muted-foreground ml-2 pointer-events-none" />
      </button>

      {open && (
        <div className="absolute z-50 top-full left-0 mt-1 w-64 max-h-[18rem] overflow-y-auto rounded-lg border border-border bg-card shadow-lg py-1">
          {options.map((opt) => {
            const isSelected = selected.includes(opt);
            return (
              <label key={opt} className="flex items-center px-3 py-2.5 cursor-pointer hover:bg-muted/50 transition-colors">
                <input 
                  type="checkbox" 
                  className="hidden" 
                  checked={isSelected}
                  onChange={() => toggleOption(opt)}
                />
                <div className={`shrink-0 w-4 h-4 rounded border flex items-center justify-center mr-3 transition-colors ${isSelected ? "bg-primary border-primary text-primary-foreground" : "border-muted-foreground"}`}>
                  {isSelected && <Check className="w-3 h-3" />}
                </div>
                <span className="text-sm font-medium leading-tight">{opt}</span>
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}
