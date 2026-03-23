"use client";

import { useEffect } from "react";
import { loadStoredDateRange, defaultStoredDateRange } from "@/lib/dateRangeStorage";
import { loadOverviewBundle } from "@/lib/overviewFetch";
import { ensureSearchDirectoryLoaded } from "@/lib/searchDirectoryCache";

/**
 * Warms sessionStorage-backed caches as soon as the app loads so Overview and the
 * transactions search bar can render from cache without waiting on first navigation.
 */
export default function AppBootstrap() {
  useEffect(() => {
    const range = loadStoredDateRange() ?? defaultStoredDateRange();
    const tasks: Promise<unknown>[] = [ensureSearchDirectoryLoaded()];
    if (range.start && range.end) {
      tasks.push(loadOverviewBundle(range.start, range.end).catch(() => {}));
    }
    void Promise.all(tasks).catch(() => {});
  }, []);

  return null;
}
