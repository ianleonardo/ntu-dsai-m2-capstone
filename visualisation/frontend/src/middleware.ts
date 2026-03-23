import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export function middleware(request: NextRequest) {
  // Intercept all requests going to `/api/*`
  if (request.nextUrl.pathname.startsWith('/api/') || request.nextUrl.pathname.startsWith('/insider-api/')) {
    
    // Retrieve the backend URL injected at runtime by Cloud Run
    const backend = (process.env.BACKEND_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    
    // Construct the correct target URL
    // e.g. "https://insider-backend.run.app" + "/api/summary" + "?start_date=..."
    const targetUrl = new URL(`${backend}${request.nextUrl.pathname}${request.nextUrl.search}`);
    
    // Transparently rewrite (proxy) the user's request to the FastAPI backend!
    return NextResponse.rewrite(targetUrl);
  }
}

// Optimization: Only run this middleware function on API paths
export const config = {
  matcher: ['/api/:path*', '/insider-api/:path*'],
};
