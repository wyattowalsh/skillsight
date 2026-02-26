import { useMemo, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";

import DetailPanel from "@/components/DetailPanel";
import MetricStrip from "@/components/MetricStrip";
import SkillTable from "@/components/SkillTable";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { useLeaderboardData } from "@/hooks/useLeaderboardData";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { SortMode } from "@/lib/api";

export default function LeaderboardPage() {
  const [searchInput, setSearchInput] = useState("");
  const [sort, setSort] = useState<SortMode>("installs");
  const [page, setPage] = useState(1);
  const [pageSize] = useState(12);
  const query = useDebouncedValue(searchInput, 300);
  const { stats, skills, selectedId, setSelectedId, selectedSkill, metrics, error, total } = useLeaderboardData({
    page,
    pageSize,
    query,
    sort,
  });

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Skills Leaderboard</h1>
        <p className="text-sm text-muted-foreground">
          Convergence-driven discovery, structured extraction, and contract-first analytics.
        </p>
      </div>

      <MetricStrip stats={stats} />

      <Card className="flex flex-col gap-3 p-4 sm:flex-row sm:items-center">
        <div className="flex-1">
          <Input
            value={searchInput}
            onChange={(event) => {
              setPage(1);
              setSearchInput(event.target.value);
            }}
            placeholder="Search skill name or id..."
            aria-label="Search skills"
          />
        </div>

        <Select
          value={sort}
          onValueChange={(value) => {
            setPage(1);
            setSort(value as SortMode);
          }}
        >
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Sort by" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="installs">Total Installs</SelectItem>
            <SelectItem value="weekly">Weekly Installs</SelectItem>
            <SelectItem value="name">Name</SelectItem>
          </SelectContent>
        </Select>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="icon-sm"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page === 1}
            aria-label="Previous page"
          >
            <ChevronLeft className="size-4" />
          </Button>
          <span className="min-w-[80px] text-center text-sm tabular-nums text-muted-foreground">
            {page} / {totalPages}
          </span>
          <Button
            variant="outline"
            size="icon-sm"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            aria-label="Next page"
          >
            <ChevronRight className="size-4" />
          </Button>
        </div>
      </Card>

      {error && (
        <Card className="border-destructive bg-destructive/5 p-4 text-center text-sm text-destructive">
          {error}
        </Card>
      )}

      <div className="grid gap-4 lg:grid-cols-[1.4fr_1fr]">
        <Card className="overflow-hidden py-0">
          <SkillTable skills={skills} selectedId={selectedId} onSelect={setSelectedId} />
        </Card>
        <div>
          <DetailPanel skill={selectedSkill} metrics={metrics} />
        </div>
      </div>
    </div>
  );
}
