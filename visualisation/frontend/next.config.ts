import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /**
   * In development, proxy /insider-api/* → FastAPI so the browser uses same-origin
   * requests (avoids CORS and localhost vs 127.0.0.1 mismatches). Override target with BACKEND_URL.
   */
  async rewrites() {
    if (process.env.NODE_ENV !== "development") return [];
    const backend = (process.env.BACKEND_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    return [{ source: "/insider-api/:path*", destination: `${backend}/api/:path*` }];
  },
};

export default nextConfig;
