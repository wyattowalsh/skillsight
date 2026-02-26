import { line, scaleLinear, scalePoint, select } from "d3";
import { useEffect, useRef } from "react";

import type { MetricItem } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

type MetricChartProps = {
  items: MetricItem[];
};

// HR-19: Dark-mode aware colors.
// Uses CSS custom properties so the chart adapts to the current theme.
// The --chart-* variables are defined by shadcn/ui's theme; we fall back
// to reasonable defaults when they are absent.
const COLOR = {
  muted: "var(--muted-foreground, oklch(0.55 0.02 230))",
  gridLine: "var(--border, oklch(0.88 0.02 75))",
  accent: "var(--chart-1, oklch(0.55 0.16 50))",
  dotStroke: "var(--card, oklch(0.99 0.005 75))",
} as const;

export default function MetricChart({ items }: MetricChartProps) {
  const ref = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    const svg = select(ref.current);
    svg.selectAll("*").remove();

    const width = 560;
    const height = 220;
    const margin = { top: 20, right: 16, bottom: 24, left: 56 };

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    if (items.length === 0) {
      svg
        .append("text")
        .attr("x", width / 2)
        .attr("y", height / 2)
        .attr("text-anchor", "middle")
        .attr("fill", COLOR.muted)
        .text("No metrics available");
      return;
    }

    const values = items.map((item) => item.total_installs ?? 0);
    const max = Math.max(...values, 1);

    const x = scalePoint()
      .domain(items.map((item) => item.snapshot_date))
      .range([margin.left, width - margin.right]);

    const y = scaleLinear().domain([0, max]).nice().range([height - margin.bottom, margin.top]);

    const l = line<MetricItem>()
      .x((d) => x(d.snapshot_date) ?? margin.left)
      .y((d) => y(d.total_installs ?? 0));

    const ticks = y.ticks(4);
    svg
      .append("g")
      .selectAll("line")
      .data(ticks)
      .join("line")
      .attr("x1", margin.left)
      .attr("x2", width - margin.right)
      .attr("y1", (d) => y(d))
      .attr("y2", (d) => y(d))
      .attr("stroke", COLOR.gridLine)
      .attr("stroke-dasharray", "3,4");

    svg
      .append("path")
      .datum(items)
      .attr("fill", "none")
      .attr("stroke", COLOR.accent)
      .attr("stroke-width", 2.5)
      .attr("stroke-linejoin", "round")
      .attr("stroke-linecap", "round")
      .attr("d", l);

    svg
      .append("g")
      .selectAll("circle")
      .data(items)
      .join("circle")
      .attr("cx", (d) => x(d.snapshot_date) ?? margin.left)
      .attr("cy", (d) => y(d.total_installs ?? 0))
      .attr("r", 4)
      .attr("fill", COLOR.accent)
      .attr("stroke", COLOR.dotStroke)
      .attr("stroke-width", 2);

    svg
      .append("g")
      .selectAll("text")
      .data(items)
      .join("text")
      .attr("x", (d) => x(d.snapshot_date) ?? margin.left)
      .attr("y", height - 6)
      .attr("text-anchor", "middle")
      .attr("font-size", 11)
      .attr("fill", COLOR.muted)
      .text((d) => d.snapshot_date.slice(5));

    svg
      .append("g")
      .selectAll("text")
      .data(ticks)
      .join("text")
      .attr("x", 8)
      .attr("y", (d) => y(d) + 3)
      .attr("font-size", 11)
      .attr("fill", COLOR.muted)
      .text((d) => Math.round(d).toLocaleString());
  }, [items]);

  return (
    <Card className="py-3">
      <CardHeader className="px-4 pb-1 pt-0">
        <CardTitle className="text-sm font-medium">Total Installs Trend</CardTitle>
      </CardHeader>
      <CardContent className="px-4 pt-0">
        <svg ref={ref} role="img" aria-label="Total installs over time" className="block h-auto w-full" />
      </CardContent>
    </Card>
  );
}
