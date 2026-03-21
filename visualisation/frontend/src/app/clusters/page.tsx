"use client";

import React from "react";
import Navbar from "@/components/Navbar";
import { Users, Info } from "lucide-react";

export default function ClustersPage() {
  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      
      <main className="container mx-auto px-4 py-8 sm:px-8">
        <div className="max-w-2xl mb-12">
          <h1 className="text-4xl font-extrabold font-headline tracking-tight text-foreground sm:text-5xl">
            Cluster Buys
          </h1>
          <p className="text-muted-foreground mt-4 text-base leading-relaxed">
            Detecting periods where multiple insiders are purchasing shares of the same company simultaneously. 
            These "clusters" are often considered one of the strongest bullish signals in insider tracking.
          </p>
        </div>

        <div className="bg-card rounded-2xl border border-border p-12 shadow-inner flex flex-col items-center justify-center text-center">
          <div className="w-16 h-16 bg-primary/10 rounded-full flex items-center justify-center mb-6">
            <Users className="w-8 h-8 text-primary" />
          </div>
          <h2 className="text-xl font-bold mb-2">Algorithm Processing</h2>
          <p className="text-sm text-muted-foreground max-w-md">
            The cluster detection algorithm is currently analyzing recent filing data to identify high-conviction group buying patterns.
          </p>
          <div className="mt-8 flex items-center gap-2 text-[10px] font-bold uppercase tracking-widest text-primary/60">
            <Info className="w-3 h-3" />
            Check back shortly for synchronized signals
          </div>
        </div>
      </main>
    </div>
  );
}
