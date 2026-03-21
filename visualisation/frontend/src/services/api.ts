const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

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
    startDate?: string,
    endDate?: string,
    min_value?: number,
    page?: number
  }) => {
    const searchParams = new URLSearchParams();
    if (params.ticker) searchParams.append('ticker', params.ticker);
    if (params.startDate) searchParams.append('start_date', params.startDate);
    if (params.endDate) searchParams.append('end_date', params.endDate);
    if (params.min_value !== undefined) searchParams.append('min_value', params.min_value.toString());
    if (params.page) searchParams.append('page', params.page.toString());

    const response = await fetch(`${API_BASE_URL}/transactions?${searchParams.toString()}`);
    return response.json();
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
  }
};
