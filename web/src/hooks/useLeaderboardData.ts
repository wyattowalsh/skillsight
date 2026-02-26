import { useEffect, useState } from "react";

import type { SkillListItem, SkillRecord } from "@/contracts/types";
import { getMetrics, getSkill, getStatsSummary, listSkills } from "@/lib/api";
import type { MetricItem, SortMode, StatsSummary } from "@/lib/api";

type UseLeaderboardDataParams = {
  page: number;
  pageSize: number;
  query: string;
  sort: SortMode;
};

type UseLeaderboardDataResult = {
  stats: StatsSummary | null;
  skills: SkillListItem[];
  selectedId: string | null;
  setSelectedId: (id: string | null) => void;
  selectedSkill: SkillRecord | null;
  metrics: MetricItem[];
  error: string | null;
  total: number;
};

export function reconcileSelectedId<T extends { id: string }>(
  selectedId: string | null,
  skills: readonly T[],
): string | null {
  if (skills.length === 0) {
    return null;
  }
  if (selectedId && skills.some((skill) => skill.id === selectedId)) {
    return selectedId;
  }
  return skills[0].id;
}

export function useLeaderboardData({
  page,
  pageSize,
  query,
  sort,
}: UseLeaderboardDataParams): UseLeaderboardDataResult {
  const [stats, setStats] = useState<StatsSummary | null>(null);
  const [skills, setSkills] = useState<SkillListItem[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedSkill, setSelectedSkill] = useState<SkillRecord | null>(null);
  const [metrics, setMetrics] = useState<MetricItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [total, setTotal] = useState(0);

  useEffect(() => {
    getStatsSummary().then(setStats).catch(() => setStats(null));
  }, []);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    listSkills(page, pageSize, query, sort)
      .then((response) => {
        if (cancelled) return;
        setSkills(response.items);
        setTotal(response.total);
        setSelectedId((current) => reconcileSelectedId(current, response.items));
      })
      .catch((err) => {
        if (cancelled) return;
        setSkills([]);
        setTotal(0);
        setError(err instanceof Error ? err.message : "Failed to load skills");
      });
    return () => {
      cancelled = true;
    };
  }, [page, pageSize, query, sort]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedId) {
      setSelectedSkill(null);
      setMetrics([]);
      return () => {
        cancelled = true;
      };
    }

    getSkill(selectedId)
      .then((value) => {
        if (!cancelled) {
          setSelectedSkill(value);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSelectedSkill(null);
        }
      });

    getMetrics(selectedId)
      .then((response) => {
        if (!cancelled) {
          setMetrics(response.items);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setMetrics([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [selectedId]);

  return {
    stats,
    skills,
    selectedId,
    setSelectedId,
    selectedSkill,
    metrics,
    error,
    total,
  };
}
