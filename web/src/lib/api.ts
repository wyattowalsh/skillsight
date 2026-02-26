import metricsMock from "@/mocks/metrics.json";
import skillDetailMock from "@/mocks/skill_detail.json";
import skillsListMock from "@/mocks/skills_list.json";
import statsSummaryMock from "@/mocks/stats_summary.json";
import statsWaveAMock from "@/mocks/stats_wave_a.json";
import type { PlatformInstalls, SkillListItem, SkillRecord } from "@/contracts/types";

type SortMode = "installs" | "weekly" | "name";

type SkillListResponse = {
  items: SkillListItem[];
  page: number;
  page_size: number;
  total: number;
};

type StatsSummary = {
  total_skills: number;
  total_repos: number;
  snapshot_date: string;
};

type EnrichmentStats = {
  snapshot_date: string;
  total_skills: number;
  history_mode: string;
  history_snapshots_considered: number;
  backfilled: {
    platform_installs: number;
    categories: number;
    first_seen_date: number;
  };
  coverage: {
    platform_installs: { count: number; pct: number };
    categories: { count: number; pct: number };
    first_seen_date: { count: number; pct: number };
  };
  ready: {
    platform_installs: boolean;
    categories: boolean;
    first_seen_date: boolean;
    wave_b: boolean;
  };
};

type RankTurbulenceMover = {
  id: string;
  name: string;
  prev_rank: number;
  curr_rank: number;
  delta_rank: number;
  total_installs: number | null;
};

type RankTurbulenceChart = {
  snapshot_date: string;
  previous_snapshot_date: string | null;
  kpis: {
    matched_skill_count: number;
    median_abs_rank_delta: number | null;
    p90_abs_rank_delta: number | null;
    improved_count: number;
    declined_count: number;
    unchanged_count: number;
  };
  buckets: Array<{ bucket: string; count: number }>;
  top_gainers: RankTurbulenceMover[];
  top_losers: RankTurbulenceMover[];
};

type MomentumVsScalePoint = {
  id: string;
  name: string;
  rank_at_fetch: number | null;
  total_installs: number;
  delta_installs: number | null;
  delta_pct: number | null;
};

type MomentumVsScaleChart = {
  snapshot_date: string;
  previous_snapshot_date: string | null;
  kpis: {
    positive_momentum_pct: number | null;
    median_delta_installs: number | null;
    p90_delta_installs: number | null;
  };
  points: MomentumVsScalePoint[];
};

type LongTailPowerCurveChart = {
  snapshot_date: string;
  kpis: {
    total_installs_sum: number;
    top10_share_pct: number;
    top50_share_pct: number;
    top100_share_pct: number;
  };
  curve: Array<{
    rank: number;
    installs: number;
    cumulative_installs: number;
    cumulative_share_pct: number;
  }>;
};

type SourceEffectivenessChart = {
  snapshot_date: string;
  kpis: {
    source_count: number;
    dominant_source: string | null;
    dominant_source_share_pct: number | null;
  };
  sources: Array<{
    source: string;
    skill_count: number;
    skill_share_pct: number;
    total_installs: number;
    median_installs: number;
  }>;
};

type DailyChangeCards = {
  snapshot_date: string;
  previous_snapshot_date: string | null;
  compared_skill_count: number;
  cards: Array<{
    id:
      | "net_install_delta"
      | "new_skills_count"
      | "dropped_skills_count"
      | "gainers_count"
      | "decliners_count"
      | "unchanged_count";
    label: string;
    value: number;
  }>;
};

type WaveAAnalyticsResponse = {
  snapshot_date: string;
  previous_snapshot_date: string | null;
  rank_turbulence: RankTurbulenceChart;
  momentum_vs_scale: MomentumVsScaleChart;
  long_tail_power_curve: LongTailPowerCurveChart;
  source_effectiveness: SourceEffectivenessChart;
  daily_change_cards: DailyChangeCards;
  limitations: string[];
};

type MetricItem = {
  id: string;
  snapshot_date: string;
  total_installs?: number | null;
  weekly_installs?: number | null;
  platform_installs?: PlatformInstalls | null;
};

type MetricsResponse = {
  id: string;
  items: MetricItem[];
};

type StaticLatestManifest = {
  format_version: number;
  snapshot_date: string;
  generated_at: string;
  page_size: number;
  sort_modes: SortMode[];
  paths?: {
    stats_wave_a?: string;
    stats_enrichment?: string;
  };
  counts?: {
    total_skills: number;
    total_repos: number;
  };
};

type SearchResponse = SkillListResponse & {
  snapshot_date: string;
};

type SearchIndexItem = {
  id: string;
  skill_id: string;
  owner: string;
  repo: string;
  name: string;
  canonical_url: string;
  total_installs?: number | null;
  weekly_installs?: number | null;
  rank_at_fetch?: number | null;
};

type SlimSearchIndex = {
  snapshot_date: string;
  items: SearchIndexItem[];
};

const LEGACY_API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";
const RAW_DATA_BASE = import.meta.env.VITE_DATA_BASE_URL ?? LEGACY_API_BASE;
const SEARCH_API_BASE = (import.meta.env.VITE_SEARCH_API_BASE_URL ?? LEGACY_API_BASE).replace(/\/$/, "");
const USE_MOCKS = String(import.meta.env.VITE_USE_MOCKS ?? "false").toLowerCase() === "true";
const DATA_ROOT = RAW_DATA_BASE.replace(/\/$/, "").endsWith("/data/v1")
  ? RAW_DATA_BASE.replace(/\/$/, "")
  : `${RAW_DATA_BASE.replace(/\/$/, "")}/data/v1`;

let manifestPromise: Promise<StaticLatestManifest> | null = null;
let slimSearchIndexPromise: Promise<SlimSearchIndex> | null = null;
const jsonRequestCache = new Map<string, Promise<unknown>>();

function cacheKey(url: string): string {
  return url;
}

function fetchCachedJson<T>(url: string): Promise<T> {
  const key = cacheKey(url);
  const existing = jsonRequestCache.get(key);
  if (existing) {
    return existing as Promise<T>;
  }
  const pending = fetchJson<T>(url).catch((err) => {
    jsonRequestCache.delete(key);
    throw err;
  });
  jsonRequestCache.set(key, pending);
  return pending;
}

function buildUrl(base: string, path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path;
  }
  if (!base) {
    return path;
  }
  return `${base}${path.startsWith("/") ? "" : "/"}${path}`;
}

function buildStaticPath(path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path;
  }

  // Manifest path templates may already include the static export prefix.
  const normalizedPath = path.startsWith("/data/v1/") ? path.slice("/data/v1".length) : path;
  return `${DATA_ROOT}${normalizedPath.startsWith("/") ? "" : "/"}${normalizedPath}`;
}

function encodeSkillPath(id: string): { owner: string; repo: string; skillId: string } {
  const parts = id.split("/");
  if (parts.length !== 3 || parts.some((p) => p.length === 0)) {
    throw new Error(`Invalid skill id: ${id}`);
  }
  const [owner, repo, skillId] = parts;
  return {
    owner: encodeURIComponent(owner),
    repo: encodeURIComponent(repo),
    skillId: encodeURIComponent(skillId),
  };
}

function pageFile(page: number): string {
  return `page-${String(page).padStart(4, "0")}.json`;
}

function demoSkillList(): SkillListItem[] {
  const seed = skillsListMock.items[0] as SkillRecord;
  const items: SkillListItem[] = [];
  for (let i = 0; i < 36; i += 1) {
    const installs = (seed.total_installs ?? 1000) + i * 187;
    const weekly = (seed.weekly_installs ?? 100) + i * 11;
    items.push({
      id: `${seed.owner}/${seed.repo}/${seed.skill_id}-${i + 1}`,
      skill_id: `${seed.skill_id}-${i + 1}`,
      owner: seed.owner,
      repo: seed.repo,
      canonical_url: String(seed.canonical_url),
      name: i % 2 === 0 ? `Agent Pattern ${i + 1}` : `Workflow Blueprint ${i + 1}`,
      description: i % 2 === 0 ? "High-signal implementation skill" : "Structured execution playbook",
      total_installs: installs,
      weekly_installs: weekly,
      rank_at_fetch: i + 1,
      platform_installs: seed.platform_installs ?? null,
    });
  }
  return items;
}

function mockListSkills(page: number, pageSize: number, q: string, sort: SortMode): SkillListResponse {
  let items = demoSkillList();
  const query = q.trim().toLowerCase();
  if (query) {
    items = items.filter((item) => item.name.toLowerCase().includes(query) || item.id.toLowerCase().includes(query));
  }

  if (sort === "name") {
    items.sort((a, b) => a.name.localeCompare(b.name));
  } else if (sort === "weekly") {
    items.sort((a, b) => (b.weekly_installs ?? 0) - (a.weekly_installs ?? 0));
  } else {
    items.sort((a, b) => (b.total_installs ?? 0) - (a.total_installs ?? 0));
  }

  const total = items.length;
  const start = (page - 1) * pageSize;
  return {
    items: items.slice(start, start + pageSize),
    page,
    page_size: pageSize,
    total,
  };
}

async function fetchJson<T>(urlOrPath: string): Promise<T> {
  const response = await fetch(urlOrPath);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

function sortListItems(items: SkillListItem[], sort: SortMode): SkillListItem[] {
  if (sort === "name") {
    return [...items].sort((a, b) => a.name.localeCompare(b.name));
  }
  if (sort === "weekly") {
    return [...items].sort((a, b) => (b.weekly_installs ?? 0) - (a.weekly_installs ?? 0));
  }
  return [...items].sort((a, b) => (b.total_installs ?? 0) - (a.total_installs ?? 0));
}

async function getLatestManifest(): Promise<StaticLatestManifest> {
  if (!manifestPromise) {
    manifestPromise = fetchCachedJson<StaticLatestManifest>(buildStaticPath("/latest.json")).catch((err) => {
      manifestPromise = null;
      throw err;
    });
  }
  return manifestPromise;
}

async function getSlimSearchIndex(): Promise<SlimSearchIndex> {
  if (!slimSearchIndexPromise) {
    const manifest = await getLatestManifest();
    slimSearchIndexPromise = fetchCachedJson<SlimSearchIndex>(
      buildStaticPath(`/snapshots/${manifest.snapshot_date}/search/slim-index.json`),
    ).catch((err) => {
      slimSearchIndexPromise = null;
      throw err;
    });
  }
  return slimSearchIndexPromise;
}

async function listSkillsFromStatic(page: number, pageSize: number, sort: SortMode): Promise<SkillListResponse> {
  const manifest = await getLatestManifest();
  if (pageSize !== manifest.page_size) {
    throw new Error(`Unsupported page size ${pageSize}; expected ${manifest.page_size}`);
  }
  const path = `/snapshots/${manifest.snapshot_date}/leaderboard/${sort}/${pageFile(page)}`;
  return fetchCachedJson<SkillListResponse>(buildStaticPath(path));
}

function toSkillListItem(item: SearchIndexItem): SkillListItem {
  return { ...item };
}

async function listSkillsFromSearchApi(page: number, pageSize: number, q: string, sort: SortMode): Promise<SkillListResponse> {
  if (!SEARCH_API_BASE) {
    // Local fallback when tiny search API is not configured.
    const index = await getSlimSearchIndex();
    const query = q.trim().toLowerCase();
    const filtered = index.items.filter((item) => item.name.toLowerCase().includes(query) || item.id.toLowerCase().includes(query));
    const sorted = sortListItems(filtered, sort);
    const start = (page - 1) * pageSize;
    return {
      items: sorted.slice(start, start + pageSize).map(toSkillListItem),
      page,
      page_size: pageSize,
      total: sorted.length,
    };
  }

  const url = buildUrl(
    SEARCH_API_BASE,
    `/v1/search?q=${encodeURIComponent(q)}&page=${page}&page_size=${pageSize}&sort=${sort}`,
  );
  const response = await fetchCachedJson<SearchResponse>(url);
  return {
    items: response.items,
    page: response.page,
    page_size: response.page_size,
    total: response.total,
  };
}

export async function listSkills(page: number, pageSize: number, q: string, sort: SortMode): Promise<SkillListResponse> {
  if (USE_MOCKS) {
    return mockListSkills(page, pageSize, q, sort);
  }

  const query = q.trim();
  if (query.length > 0) {
    return listSkillsFromSearchApi(page, pageSize, query, sort);
  }
  return listSkillsFromStatic(page, pageSize, sort);
}

export async function getSkill(id: string): Promise<SkillRecord> {
  if (USE_MOCKS) {
    return { ...skillDetailMock, id } as SkillRecord;
  }

  const manifest = await getLatestManifest();
  const { owner, repo, skillId } = encodeSkillPath(id);
  return fetchCachedJson<SkillRecord>(
    buildStaticPath(`/snapshots/${manifest.snapshot_date}/skills/by-id/${owner}/${repo}/${skillId}.json`),
  );
}

export async function getStatsSummary(): Promise<StatsSummary> {
  if (USE_MOCKS) {
    return statsSummaryMock as StatsSummary;
  }

  const manifest = await getLatestManifest();
  return fetchCachedJson<StatsSummary>(buildStaticPath(`/snapshots/${manifest.snapshot_date}/stats/summary.json`));
}

export async function getWaveAAnalytics(): Promise<WaveAAnalyticsResponse> {
  if (USE_MOCKS) {
    return statsWaveAMock as WaveAAnalyticsResponse;
  }

  const manifest = await getLatestManifest();
  const statsWaveAPath = manifest.paths?.stats_wave_a ?? `/snapshots/${manifest.snapshot_date}/stats/wave-a.json`;
  return fetchCachedJson<WaveAAnalyticsResponse>(buildStaticPath(statsWaveAPath));
}

export async function getEnrichmentStats(): Promise<EnrichmentStats> {
  if (USE_MOCKS) {
    const summary = statsSummaryMock as StatsSummary;
    return {
      snapshot_date: summary.snapshot_date,
      total_skills: summary.total_skills,
      history_mode: "previous_snapshot",
      history_snapshots_considered: 1,
      backfilled: {
        platform_installs: 0,
        categories: 0,
        first_seen_date: 0,
      },
      coverage: {
        platform_installs: { count: 0, pct: 0 },
        categories: { count: 0, pct: 0 },
        first_seen_date: { count: 0, pct: 0 },
      },
      ready: {
        platform_installs: false,
        categories: false,
        first_seen_date: false,
        wave_b: false,
      },
    };
  }

  const manifest = await getLatestManifest();
  const enrichmentPath =
    manifest.paths?.stats_enrichment ?? `/snapshots/${manifest.snapshot_date}/stats/enrichment.json`;
  return fetchCachedJson<EnrichmentStats>(buildStaticPath(enrichmentPath));
}

export async function getMetrics(id: string): Promise<MetricsResponse> {
  if (USE_MOCKS) {
    return {
      ...(metricsMock as MetricsResponse),
      id,
    };
  }

  const manifest = await getLatestManifest();
  const { owner, repo, skillId } = encodeSkillPath(id);
  return fetchCachedJson<MetricsResponse>(
    buildStaticPath(`/snapshots/${manifest.snapshot_date}/metrics/by-id/${owner}/${repo}/${skillId}.json`),
  );
}

export type {
  EnrichmentStats,
  DailyChangeCards,
  MetricItem,
  MetricsResponse,
  MomentumVsScaleChart,
  RankTurbulenceChart,
  SkillListResponse,
  SortMode,
  SourceEffectivenessChart,
  StatsSummary,
  WaveAAnalyticsResponse,
};
