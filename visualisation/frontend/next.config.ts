import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // API Rewrites are instead handled dynamically at runtime via src/middleware.ts 
  // to ensure process.env.BACKEND_URL from Cloud Run is respected!
};

export default nextConfig;
