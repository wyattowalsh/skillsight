import { Card, CardContent } from "@/components/ui/card";
import type { StatsSummary } from "@/lib/api";

type MetricStripProps = {
  stats: StatsSummary | null;
};

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <Card className="py-4">
      <CardContent className="px-4">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {label}
        </p>
        <p className="mt-1 font-mono text-xl font-semibold">{value}</p>
      </CardContent>
    </Card>
  );
}

export default function MetricStrip({ stats }: MetricStripProps) {
  const totalSkills = stats ? stats.total_skills.toLocaleString() : "--";
  const totalRepos = stats ? stats.total_repos.toLocaleString() : "--";
  const snapshotDate = stats ? stats.snapshot_date : "--";

  return (
    <section className="grid grid-cols-1 gap-3 sm:grid-cols-3" aria-label="Summary metrics">
      <MetricCard label="Total Skills" value={totalSkills} />
      <MetricCard label="Total Repos" value={totalRepos} />
      <MetricCard label="Snapshot Date" value={snapshotDate} />
    </section>
  );
}
