/**
 * - If NEXT_PUBLIC_API_URL is set, use it (trim trailing slash).
 * - In development, default to FastAPI directly on 127.0.0.1:8000 — Next.js rewrites
 *   often hit a short proxy timeout; long BigQuery calls then ECONNRESET before JSON returns.
 *   To use the same-origin rewrite instead, set NEXT_PUBLIC_API_URL=/insider-api
 */
function apiBaseUrl(): string {
  const env = process.env.NEXT_PUBLIC_API_URL?.trim().replace(/\/$/, "");
  if (env) return env;
  return "http://127.0.0.1:8000/api";
}

const API_BASE_URL = apiBaseUrl();

async function parseJsonResponse(response: Response, label: string): Promise<unknown> {
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${label} ${response.status}: ${text.slice(0, 200)}`);
  }
  if (!text.trim()) {
    throw new Error(`${label}: empty response body`);
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new Error(`${label}: invalid JSON (${text.slice(0, 100)}…)`);
  }
}

export const api = {
  getSummary: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await fetch(`${API_BASE_URL}/summary?${params.toString()}`);
    return response.json();
  },
  getTransactions: async (params: {
    ticker?: string,
    search?: string,
    startDate?: string,
    endDate?: string,
    min_value?: number,
    page?: number,
    page_size?: number,
  }) => {
    const searchParams = new URLSearchParams();
    if (params.ticker) searchParams.append('ticker', params.ticker);
    if (params.search) searchParams.append('search', params.search);
    if (params.startDate) searchParams.append('start_date', params.startDate);
    if (params.endDate) searchParams.append('end_date', params.endDate);
    if (params.min_value !== undefined) searchParams.append('min_value', params.min_value.toString());
    if (params.page) searchParams.append('page', params.page.toString());
    if (params.page_size != null) searchParams.append('page_size', String(params.page_size));

    const response = await fetch(`${API_BASE_URL}/transactions?${searchParams.toString()}`);
    return parseJsonResponse(response, "transactions") as Promise<{
      data?: unknown[];
      total?: number;
      page?: number;
      page_size?: number;
      has_more?: boolean;
    }>;
  },
  getTickers: async () => {
    const response = await fetch(`${API_BASE_URL}/tickers`);
    return response.json();
  },
  getOwners: async () => {
    const response = await fetch(`${API_BASE_URL}/owners`);
    return response.json();
  },
  getTopTransactions: async (startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);
    const response = await fetch(`${API_BASE_URL}/top-transactions?${params.toString()}`);
    return response.json();
  },

  getTickerChart: async (ticker: string) => {
    const response = await fetch(`${API_BASE_URL}/stocks/${ticker}/chart`);
    return parseJsonResponse(response, "ticker chart") as Promise<{
      data?: Array<{ 
        time: string; 
        open: number; high: number; low: number; close: number;
        sma200: number | null;
        macd: number | null;
        macd_signal: number | null;
        macd_hist: number | null;
      }>;
    }>;
  },

  getSp500Companies: async () => {
    const response = await fetch(`${API_BASE_URL}/sp500-companies`);
    return response.json();
  },

  /** Loads stocks + insiders in parallel (split API — single combined JSON was ~40MB+ and broke browsers). */
  getSearchDirectory: async () => {
    const [stocksRes, insidersRes] = await Promise.all([
      fetch(`${API_BASE_URL}/search-directory/stocks`),
      fetch(`${API_BASE_URL}/search-directory/insiders`),
    ]);
    const [sBody, iBody] = await Promise.all([
      parseJsonResponse(stocksRes, "search-directory/stocks"),
      parseJsonResponse(insidersRes, "search-directory/insiders"),
    ]);
    const s = sBody as { stocks?: unknown[] };
    const i = iBody as { insiders?: unknown[] };
    return {
      stocks: Array.isArray(s.stocks) ? s.stocks : [],
      insiders: Array.isArray(i.insiders) ? i.insiders : [],
    };
  },

  getClusters: async (params: {
    side: 'buy' | 'sell';
    startDate?: string;
    endDate?: string;
    min_filings?: number;
    limit?: number;
    ticker?: string;
    search?: string;
  }) => {
    const sp = new URLSearchParams();
    sp.append('side', params.side);
    if (params.startDate) sp.append('start_date', params.startDate);
    if (params.endDate) sp.append('end_date', params.endDate);
    if (params.min_filings != null) sp.append('min_filings', String(params.min_filings));
    if (params.limit != null) sp.append('limit', String(params.limit));
    if (params.ticker) sp.append('ticker', params.ticker);
    if (params.search) sp.append('search', params.search);
    const response = await fetch(`${API_BASE_URL}/clusters?${sp.toString()}`);
    return parseJsonResponse(response, "clusters") as Promise<{
      data?: unknown[];
      side?: string;
      start_date?: string;
      end_date?: string;
    }>;
  },

  getClusterBreakdown: async (params: {
    side: "buy" | "sell";
    ticker: string;
    weekStart: string;
    startDate?: string;
    endDate?: string;
  }) => {
    const sp = new URLSearchParams();
    sp.append("side", params.side);
    sp.append("ticker", params.ticker);
    sp.append("week_start", params.weekStart);
    if (params.startDate) sp.append("start_date", params.startDate);
    if (params.endDate) sp.append("end_date", params.endDate);
    const response = await fetch(`${API_BASE_URL}/clusters/breakdown?${sp.toString()}`);
    return parseJsonResponse(response, "cluster breakdown") as Promise<{
      data?: Array<{
        insider_name?: string;
        role?: string;
        trans_date?: string;
        amount_usd?: number;
      }>;
    }>;
  },
};
