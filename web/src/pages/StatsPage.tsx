import { useEffect, useState } from "react";

import MetricStrip from "@/components/MetricStrip";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getStatsSummary } from "@/lib/api";
import type { StatsSummary } from "@/lib/api";

export default function StatsPage() {
  const [stats, setStats] = useState<StatsSummary | null>(null);

  useEffect(() => {
    getStatsSummary().then(setStats).catch(() => setStats(null));
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Analytics</h1>
        <p className="text-sm text-muted-foreground">
          Aggregate statistics and platform insights.
        </p>
      </div>

      <MetricStrip stats={stats} />

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Platform Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex h-48 items-center justify-center text-sm text-muted-foreground">
              Platform distribution chart will be built in Task #7.
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Growth Over Time</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex h-48 items-center justify-center text-sm text-muted-foreground">
              Growth time-series will be built in Task #7.
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
