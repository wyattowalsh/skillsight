import metricsMock from "@/mocks/metrics.json";
import skillDetailMock from "@/mocks/skill_detail.json";
import skillsListMock from "@/mocks/skills_list.json";
import statsSummaryMock from "@/mocks/stats_summary.json";
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
  if (DATA_ROOT.startsWith("http")) {
    return `${DATA_ROOT}${path}`;
  }
  return `${DATA_ROOT}${path}`;
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

export type { MetricItem, MetricsResponse, SkillListResponse, SortMode, StatsSummary };
