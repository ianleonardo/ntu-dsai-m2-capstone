import React from "react";
import { ArrowUpCircle, ArrowDownCircle, Percent } from "lucide-react";
import { formatMillionsDisplay } from "@/lib/utils";

interface StatsGridProps {
  stats: {
    purchase_value_m: number;
    purchase_count: number;
    sales_value_m: number;
    sales_count: number;
  };
}

export default function StatsGrid({ stats }: StatsGridProps) {
  const totalValue = stats.purchase_value_m + stats.sales_value_m;
  const purchasePercent = totalValue > 0 ? (stats.purchase_value_m / totalValue) * 100 : 0;
  const salesPercent = totalValue > 0 ? (stats.sales_value_m / totalValue) * 100 : 0;

  return (
    <div className="bg-card p-6 rounded-2xl border border-border shadow-md h-full flex flex-col justify-between">
      <div className="flex justify-between items-center mb-6">
        <h3 className="text-sm font-bold text-muted-foreground uppercase tracking-wider">
          Market Volume Activity
        </h3>
        <Percent className="w-4 h-4 text-primary" />
      </div>

      <div className="grid grid-cols-2 gap-8">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <ArrowUpCircle className="w-5 h-5 text-emerald-500" />
            <span className="text-sm font-medium">Purchases</span>
          </div>
          <div className="text-3xl font-bold font-headline">${formatMillionsDisplay(stats.purchase_value_m)}M</div>
          <div className="text-xs text-muted-foreground mt-1">
            {stats.purchase_count.toLocaleString("en-US")} Companies
          </div>
        </div>

        <div>
          <div className="flex items-center gap-2 mb-2">
            <ArrowDownCircle className="w-5 h-5 text-rose-500" />
            <span className="text-sm font-medium">Sales</span>
          </div>
          <div className="text-3xl font-bold font-headline">${formatMillionsDisplay(stats.sales_value_m)}M</div>
          <div className="text-xs text-muted-foreground mt-1">
            {stats.sales_count.toLocaleString("en-US")} Companies
          </div>
        </div>
      </div>

      {/* Proportion Bar */}
      <div className="mt-8">
        <div className="flex justify-between text-[10px] font-bold uppercase tracking-tighter mb-2">
          <span className="text-emerald-500">{purchasePercent.toFixed(1)}% Purchase</span>
          <span className="text-rose-500">{salesPercent.toFixed(1)}% Sales</span>
        </div>
        <div className="h-3 w-full bg-secondary rounded-full overflow-hidden flex">
          <div
            className="h-full bg-emerald-500 transition-all duration-1000"
            style={{ width: `${purchasePercent}%` }}
          />
          <div
            className="h-full bg-rose-500 transition-all duration-1000"
            style={{ width: `${salesPercent}%` }}
          />
        </div>
      </div>
    </div>
  );
}
