"use client";

import { useEffect, useRef } from "react";
import { loadStoredDateRange, defaultStoredDateRange } from "@/lib/dateRangeStorage";
import { loadOverviewBundle } from "@/lib/overviewFetch";
import { ensureSearchDirectoryLoaded } from "@/lib/searchDirectoryCache";

/**
 * Component that warms up global caches / filters on mount.
 * Returns null as it has no UI.
 */
export default function AppBootstrap() {
  const initialized = useRef(false);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;

    const range = loadStoredDateRange() ?? defaultStoredDateRange();
    
    // Warm search directory
    void ensureSearchDirectoryLoaded().catch((e) => {
      console.warn("[Bootstrap] Directory search cache failed to warm:", e.message || e);
    });

    // Warm overview data
    if (range.start && range.end) {
      void loadOverviewBundle(range.start, range.end).catch((e) => {
        console.warn("[Bootstrap] Overview data failed to warm:", e.message || e);
      });
    }
  }, []);

  return null;
}
