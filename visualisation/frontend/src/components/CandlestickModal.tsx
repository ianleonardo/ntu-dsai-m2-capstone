"use client";

import React, { useEffect, useRef, useState } from "react";
import { createChart, IChartApi, Time, CandlestickSeries, createSeriesMarkers, LineSeries, HistogramSeries, CandlestickData, LineData, HistogramData, WhitespaceData } from "lightweight-charts";
import { X, Loader2 } from "lucide-react";
import { api } from "@/services/api";

const formatTransDate = (isoStr: string) => {
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return isoStr;
  return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short", year: "numeric" }).format(d);
};

interface Props {
  ticker: string;
  transDate: string; // ISO date YYYY-MM-DD
  onClose: () => void;
}

export default function CandlestickModal({ ticker, transDate, onClose }: Props) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const [loading, setLoading] = useState(true);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    let isMounted = true;
    (async () => {
      try {
        const res = await api.getTickerChart(ticker);
        if (!isMounted) return;
        const data = res.data || [];
        
        if (chartContainerRef.current) {
          const chart = createChart(chartContainerRef.current, {
            layout: { background: { color: 'transparent' }, textColor: '#94a3b8' },
            grid: {
              vertLines: { color: 'rgba(51, 65, 85, 0.4)' },
              horzLines: { color: 'rgba(51, 65, 85, 0.4)' },
            },
            timeScale: { 
              timeVisible: true,
              borderColor: 'rgba(51, 65, 85, 0.4)',
            },
            rightPriceScale: {
              borderColor: 'rgba(51, 65, 85, 0.4)',
              scaleMargins: {
                top: 0.1,
                bottom: 0.25,
              },
            },
            autoSize: true,
          });

          const series = chart.addSeries(CandlestickSeries, {
            upColor: '#22c55e',
            downColor: '#ef4444',
            borderVisible: false,
            wickUpColor: '#22c55e',
            wickDownColor: '#ef4444',
          });

          const smaSeries = chart.addSeries(LineSeries, {
            color: '#eab308', // yellow-500
            lineWidth: 2,
            crosshairMarkerVisible: false,
          });

          const macdHistSeries = chart.addSeries(HistogramSeries, {
            priceScaleId: 'macd',
            color: '#10b981', // green for positive
          });

          const macdLineSeries = chart.addSeries(LineSeries, {
            priceScaleId: 'macd',
            color: '#3b82f6', // blue
            lineWidth: 2,
          });

          const macdSignalSeries = chart.addSeries(LineSeries, {
            priceScaleId: 'macd',
            color: '#f97316', // orange
            lineWidth: 2,
          });

          // Apply scale margins to MACD scale AFTER the series are created
          chart.priceScale('macd').applyOptions({
            scaleMargins: {
              top: 0.8,
              bottom: 0,
            },
          });

          // Sort data by time ascending
          const sorted = [...data].sort((a,b) => a.time.localeCompare(b.time));
          series.setData(sorted as unknown as CandlestickData[]);

          const smaData: (LineData | WhitespaceData)[] = [];
          const macdLineData: (LineData | WhitespaceData)[] = [];
          const macdSignalData: (LineData | WhitespaceData)[] = [];
          const macdHistData: (HistogramData | WhitespaceData)[] = [];

          sorted.forEach(d => {
            const time = d.time as Time;
            smaData.push(d.sma200 != null ? { time, value: d.sma200 } : { time });
            macdLineData.push(d.macd != null ? { time, value: d.macd } : { time });
            macdSignalData.push(d.macd_signal != null ? { time, value: d.macd_signal } : { time });
            macdHistData.push(d.macd_hist != null ? { time, value: d.macd_hist, color: d.macd_hist > 0 ? '#10b981' : '#ef4444' } : { time });
          });

          smaSeries.setData(smaData as LineData[]);
          macdLineSeries.setData(macdLineData as LineData[]);
          macdSignalSeries.setData(macdSignalData as LineData[]);
          macdHistSeries.setData(macdHistData as HistogramData[]);

          // Add a marker exactly on the closest data point
          const transTime = transDate.split("T")[0];
          const validTimes = sorted.map(d => d.time);
          let markerTime = validTimes.find(t => t >= transTime);
          if (!markerTime && validTimes.length > 0) {
            markerTime = validTimes[validTimes.length - 1]; 
          }
          if (markerTime) {
            createSeriesMarkers(series, [
              {
                time: markerTime as Time,
                position: 'aboveBar',
                color: '#3b82f6',
                shape: 'arrowDown',
                text: 'Transaction Date',
              }
            ]);
          }

          // Initial fit
          chart.timeScale().fitContent();

          // After a short delay to ensure rendering, zoom to exactly 2 months before/after
          setTimeout(() => {
            if (chartRef.current && sorted.length > 0) {
              const d = new Date(transDate);
              const startObj = new Date(d);
              startObj.setMonth(startObj.getMonth() - 2);
              const endObj = new Date(d);
              endObj.setMonth(endObj.getMonth() + 2);
              const startStr = startObj.toISOString().split("T")[0];
              const endStr = endObj.toISOString().split("T")[0];

              chartRef.current.timeScale().setVisibleRange({
                from: startStr as Time,
                to: endStr as Time,
              });
            }
          }, 50);

          chartRef.current = chart;
        }
      } catch(e) {
        console.error(e);
      } finally {
        if (isMounted) setLoading(false);
      }
    })();
    
    return () => {
      isMounted = false;
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [ticker, transDate]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm sm:p-8">
      <div 
        className="relative w-full max-w-4xl bg-card border border-border/80 shadow-[0_0_100px_-20px_rgba(0,0,0,0.5)] rounded-2xl overflow-hidden flex flex-col h-[600px] max-h-[90vh] animate-in fade-in zoom-in-95 duration-200"
      >
        <div className="flex items-center justify-between px-6 py-4 border-b border-border/60 bg-muted/20">
          <div>
            <h2 className="text-xl font-bold tracking-tight text-foreground flex items-center gap-2">
              <span className="text-primary">{ticker}</span>
              <span className="text-muted-foreground font-normal text-sm border-l border-border pl-2 ml-1">
                Daily Price History
              </span>
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5 font-medium">
              Transaction Date: {formatTransDate(transDate)}
            </p>
          </div>
          <button 
            type="button" 
            onClick={onClose}
            className="p-2 -mr-2 rounded-full hover:bg-muted text-muted-foreground hover:text-foreground transition-all"
            aria-label="Close chart"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        
        <div className="flex-1 relative w-full h-full bg-card p-4 flex flex-col">
          <div className="mb-2 shrink-0 flex flex-wrap items-center gap-2 text-[10px] sm:text-xs">
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-2 w-2 rounded-sm bg-emerald-500" aria-hidden />
              Candle Up
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-2 w-2 rounded-sm bg-rose-500" aria-hidden />
              Candle Down
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-[2px] w-3 rounded bg-yellow-500" aria-hidden />
              SMA(200)
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-[2px] w-3 rounded bg-blue-500" aria-hidden />
              MACD
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-[2px] w-3 rounded bg-orange-500" aria-hidden />
              Signal
            </span>
            <span className="inline-flex items-center gap-1 rounded-full border border-border/70 bg-muted/40 px-2 py-0.5 text-muted-foreground">
              <span className="inline-block h-2 w-2 rounded-sm bg-emerald-500" aria-hidden />
              <span className="inline-block h-2 w-2 rounded-sm bg-rose-500" aria-hidden />
              Hist (+/-)
            </span>
          </div>
          {loading && (
            <div className="absolute inset-0 flex items-center justify-center bg-card/60 backdrop-blur-[1px] z-10 transition-opacity">
              <Loader2 className="w-8 h-8 animate-spin text-primary opacity-80" />
            </div>
          )}
          <div className="relative flex-1 min-h-0">
            <div ref={chartContainerRef} className="absolute inset-0" />
          </div>
        </div>
      </div>
    </div>
  );
}
