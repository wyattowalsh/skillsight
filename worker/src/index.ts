export interface Env {
  SKILLSIGHT_DATA?: R2Bucket;
}

type PlatformInstalls = {
  opencode?: number | null;
  codex?: number | null;
  gemini_cli?: number | null;
  github_copilot?: number | null;
  amp?: number | null;
  kimi_cli?: number | null;
  [key: string]: number | null | undefined;
};

type SkillListItem = {
  id: string;
  skill_id: string;
  owner: string;
  repo: string;
  name: string;
  canonical_url: string;
  total_installs?: number | null;
  weekly_installs?: number | null;
  rank_at_fetch?: number | null;
  description?: string | null;
  platform_installs?: PlatformInstalls | null;
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

type SearchIndexDocument = {
  snapshot_date: string;
  items: SearchIndexItem[];
};

type SearchResponse = {
  items: SkillListItem[];
  page: number;
  page_size: number;
  total: number;
  snapshot_date: string;
};

type SortMode = "installs" | "weekly" | "name";

type LatestManifest = {
  snapshot_date: string;
  page_size?: number;
  counts?: {
    total_skills?: number;
    total_repos?: number;
  };
};

type SkillPathParts = {
  id: string;
  owner: string;
  repo: string;
  skillId: string;
};

type SnapshotLatestManifest = {
  date: string;
};

type SnapshotStatsSummary = {
  total_skills: number;
  total_repos: number;
  snapshot_date: string;
};

type SnapshotSkillsManifest = {
  snapshot_date: string;
  total_skills: number;
  page_size: number;
  page_count: number;
};

type SnapshotSkillPageItem = {
  source: string;
  skillId: string;
  name: string;
  installs?: number | null;
};

type SnapshotSkillLookup = {
  page: number;
  index: number;
};

type SnapshotSkillLookupDocument = {
  snapshot_date: string;
  total_entries: number;
  entries: Record<string, SnapshotSkillLookup>;
};

const DATA_PREFIX = "data/v1";
const SNAPSHOTS_PREFIX = "snapshots";
const LATEST_MANIFEST_KEY = `${DATA_PREFIX}/latest.json`;
const LATEST_SNAPSHOT_KEY = `${SNAPSHOTS_PREFIX}/latest.json`;
const SEARCH_INDEX_KEY = (snapshotDate: string) =>
  `${DATA_PREFIX}/snapshots/${snapshotDate}/search/slim-index.json`;
const STATS_SUMMARY_KEY = (snapshotDate: string) =>
  `${DATA_PREFIX}/snapshots/${snapshotDate}/stats/summary.json`;
const LEADERBOARD_PAGE_KEY = (snapshotDate: string, sort: SortMode, page: number) =>
  `${DATA_PREFIX}/snapshots/${snapshotDate}/leaderboard/${sort}/page-${String(page).padStart(4, "0")}.json`;
const SKILL_DETAIL_KEY = (snapshotDate: string, owner: string, repo: string, skillId: string) =>
  `${DATA_PREFIX}/snapshots/${snapshotDate}/skills/by-id/${owner}/${repo}/${skillId}.json`;
const METRICS_KEY = (snapshotDate: string, owner: string, repo: string, skillId: string) =>
  `${DATA_PREFIX}/snapshots/${snapshotDate}/metrics/by-id/${owner}/${repo}/${skillId}.json`;
const SNAPSHOT_STATS_SUMMARY_KEY = (snapshotDate: string) =>
  `${SNAPSHOTS_PREFIX}/${snapshotDate}/stats_summary.json`;
const SNAPSHOT_SKILLS_MANIFEST_KEY = (snapshotDate: string) =>
  `${SNAPSHOTS_PREFIX}/${snapshotDate}/skills_manifest.json`;
const SNAPSHOT_SKILLS_PAGE_KEY = (snapshotDate: string, page: number) =>
  `${SNAPSHOTS_PREFIX}/${snapshotDate}/skills_pages/page-${String(page).padStart(4, "0")}.json`;
const SNAPSHOT_SKILL_LOOKUP_KEY = (snapshotDate: string) =>
  `${SNAPSHOTS_PREFIX}/${snapshotDate}/skill_lookup.json`;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const RATE_LIMIT_WINDOW_MS = 60_000;
const RATE_LIMIT_MAX_REQUESTS = 60;
const RATE_LIMIT_CLIENT_HEADER = "CF-Connecting-IP";
const SORT_MODES = new Set<SortMode>(["installs", "weekly", "name"]);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const IN_MEMORY_CACHE_TTL_MS = 60_000;

type RateLimitState = {
  windowStartMs: number;
  count: number;
};

type ParsedCache = {
  key: string;
  loadedAtMs: number;
  doc: SearchIndexDocument;
};

const rateLimitByClient = new Map<string, RateLimitState>();
let lastRateLimitSweepMs = 0;
let parsedSearchCache: ParsedCache | null = null;
let cachedLatestManifest: { loadedAtMs: number; manifest: LatestManifest } | null = null;

export function resetWorkerStateForTests(): void {
  rateLimitByClient.clear();
  lastRateLimitSweepMs = 0;
  parsedSearchCache = null;
  cachedLatestManifest = null;
}

function json(
  data: unknown,
  opts: { status?: number; headers?: Record<string, string> } = {},
): Response {
  const { status = 200, headers: extra } = opts;
  const headers: Record<string, string> = {
    "content-type": "application/json; charset=utf-8",
    "X-Content-Type-Options": "nosniff",
    ...CORS_HEADERS,
    ...extra,
  };
  if (status >= 200 && status < 300 && !("cache-control" in Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v])))) {
    headers["cache-control"] = "public, max-age=60, s-maxage=60";
  }
  return new Response(JSON.stringify(data), { status, headers });
}

function isSortMode(value: string): value is SortMode {
  return SORT_MODES.has(value as SortMode);
}

function isDateString(value: string): boolean {
  if (!DATE_RE.test(value)) return false;
  const [y, m, d] = value.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return (
    dt.getUTCFullYear() === y &&
    dt.getUTCMonth() === m - 1 &&
    dt.getUTCDate() === d
  );
}

function parsePositiveInt(value: string | null, fallback: number): number {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.floor(num);
}

function sortItems(items: SearchIndexItem[], sort: SortMode): SearchIndexItem[] {
  if (sort === "name") {
    return [...items].sort((a, b) => a.name.localeCompare(b.name));
  }
  if (sort === "weekly") {
    return [...items].sort((a, b) => (b.weekly_installs ?? 0) - (a.weekly_installs ?? 0));
  }
  return [...items].sort((a, b) => (b.total_installs ?? 0) - (a.total_installs ?? 0));
}

function parseSource(source: string): { owner: string; repo: string } {
  const [owner, ...rest] = source.split("/");
  if (!owner || rest.length === 0) {
    return { owner: source, repo: source };
  }
  return { owner, repo: rest.join("/") };
}

function snapshotItemToSkillListItem(item: SnapshotSkillPageItem, rank: number): SkillListItem {
  const { owner, repo } = parseSource(item.source);
  return {
    id: `${item.source}/${item.skillId}`,
    skill_id: item.skillId,
    owner,
    repo,
    name: item.name,
    canonical_url: `https://skills.sh/${item.source}/${item.skillId}`,
    total_installs: item.installs ?? null,
    weekly_installs: null,
    rank_at_fetch: rank,
    description: null,
    platform_installs: null,
  };
}

function getRateLimitClientId(request: Request): string {
  const clientIp = request.headers.get(RATE_LIMIT_CLIENT_HEADER)?.trim();
  return clientIp && clientIp.length > 0 ? clientIp : "unknown-client";
}

function sweepExpiredRateLimitEntries(nowMs: number): void {
  if (nowMs - lastRateLimitSweepMs < RATE_LIMIT_WINDOW_MS) return;
  lastRateLimitSweepMs = nowMs;
  for (const [clientId, state] of rateLimitByClient.entries()) {
    if (nowMs - state.windowStartMs >= RATE_LIMIT_WINDOW_MS) {
      rateLimitByClient.delete(clientId);
    }
  }
}

function rateLimitHeaders(limit: number, remaining: number, resetEpochSeconds: number): Record<string, string> {
  return {
    "X-RateLimit-Limit": String(limit),
    "X-RateLimit-Remaining": String(Math.max(0, remaining)),
    "X-RateLimit-Reset": String(resetEpochSeconds),
  };
}

function enforceRateLimit(request: Request): { blocked: Response | null; headers: Record<string, string> } {
  const nowMs = Date.now();
  sweepExpiredRateLimitEntries(nowMs);

  const clientId = getRateLimitClientId(request);
  const current = rateLimitByClient.get(clientId);
  if (!current || nowMs - current.windowStartMs >= RATE_LIMIT_WINDOW_MS) {
    const next = { windowStartMs: nowMs, count: 1 };
    rateLimitByClient.set(clientId, next);
    const resetEpochSeconds = Math.ceil((next.windowStartMs + RATE_LIMIT_WINDOW_MS) / 1000);
    return {
      blocked: null,
      headers: rateLimitHeaders(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_MAX_REQUESTS - next.count, resetEpochSeconds),
    };
  }

  if (current.count >= RATE_LIMIT_MAX_REQUESTS) {
    const retryAfterSeconds = Math.max(
      1,
      Math.ceil((current.windowStartMs + RATE_LIMIT_WINDOW_MS - nowMs) / 1000),
    );
    const resetEpochSeconds = Math.ceil((current.windowStartMs + RATE_LIMIT_WINDOW_MS) / 1000);
    return {
      blocked: json(
        { error: "Too Many Requests", retry_after_seconds: retryAfterSeconds },
        {
          status: 429,
          headers: {
            "Retry-After": String(retryAfterSeconds),
            ...rateLimitHeaders(RATE_LIMIT_MAX_REQUESTS, 0, resetEpochSeconds),
          },
        },
      ),
      headers: {},
    };
  }

  current.count += 1;
  const resetEpochSeconds = Math.ceil((current.windowStartMs + RATE_LIMIT_WINDOW_MS) / 1000);
  return {
    blocked: null,
    headers: rateLimitHeaders(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_MAX_REQUESTS - current.count, resetEpochSeconds),
  };
}

function cacheRequest(request: Request, suffix: string): Request {
  return new Request(new URL(`/cache/${suffix}`, request.url).toString());
}

async function loadJsonTextWithCache(
  env: Env,
  key: string,
  request: Request,
  cacheSuffix: string,
): Promise<string | null> {
  const bucket = env.SKILLSIGHT_DATA;
  if (!bucket) return null;

  const cache = caches.default;
  const cacheKey = cacheRequest(request, cacheSuffix);
  const cached = await cache.match(cacheKey);
  if (cached) {
    return cached.text();
  }

  const object = await bucket.get(key);
  if (!object) return null;
  const text = await object.text();
  const response = new Response(text, {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "s-maxage=60",
    },
  });
  await cache.put(cacheKey, response.clone());
  return text;
}

function parseJson<T>(text: string, label: string): T {
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new Error(`Invalid JSON in ${label}`);
  }
}

async function loadJsonObjectWithCache<T>(
  env: Env,
  key: string,
  request: Request,
  cacheSuffix: string,
): Promise<T | null> {
  const text = await loadJsonTextWithCache(env, key, request, cacheSuffix);
  if (!text) return null;
  return parseJson<T>(text, key);
}

function parseSkillPath(pathname: string, prefix: string): SkillPathParts | null {
  if (!pathname.startsWith(prefix)) return null;
  const encoded = pathname.slice(prefix.length);
  if (!encoded) return null;
  let decoded: string;
  try {
    decoded = decodeURIComponent(encoded);
  } catch {
    return null;
  }
  const parts = decoded.split("/");
  if (parts.length !== 3 || parts.some((part) => part.length === 0)) return null;
  const [owner, repo, skillId] = parts;
  return { id: decoded, owner, repo, skillId };
}

async function loadSnapshotSkillsManifest(
  env: Env,
  snapshotDate: string,
  request: Request,
): Promise<SnapshotSkillsManifest | null> {
  return loadJsonObjectWithCache<SnapshotSkillsManifest>(
    env,
    SNAPSHOT_SKILLS_MANIFEST_KEY(snapshotDate),
    request,
    `snapshot-skills-manifest/${snapshotDate}`,
  );
}

async function loadSnapshotSummary(
  env: Env,
  snapshotDate: string,
  request: Request,
): Promise<SnapshotStatsSummary | null> {
  return loadJsonObjectWithCache<SnapshotStatsSummary>(
    env,
    SNAPSHOT_STATS_SUMMARY_KEY(snapshotDate),
    request,
    `snapshot-summary/${snapshotDate}`,
  );
}

async function loadSnapshotSkillsPage(
  env: Env,
  snapshotDate: string,
  page: number,
  request: Request,
): Promise<SnapshotSkillPageItem[] | null> {
  return loadJsonObjectWithCache<SnapshotSkillPageItem[]>(
    env,
    SNAPSHOT_SKILLS_PAGE_KEY(snapshotDate, page),
    request,
    `snapshot-skills-page/${snapshotDate}/${page}`,
  );
}

async function loadSnapshotSkillLookup(
  env: Env,
  snapshotDate: string,
  request: Request,
): Promise<SnapshotSkillLookupDocument | null> {
  return loadJsonObjectWithCache<SnapshotSkillLookupDocument>(
    env,
    SNAPSHOT_SKILL_LOOKUP_KEY(snapshotDate),
    request,
    `snapshot-skill-lookup/${snapshotDate}`,
  );
}

async function buildSnapshotSkillRecord(
  env: Env,
  snapshotDate: string,
  skillId: string,
  request: Request,
): Promise<Record<string, unknown> | null> {
  const parts = skillId.split("/");
  if (parts.length !== 3 || parts.some((part) => part.length === 0)) return null;
  const [owner, repo, leafSkillId] = parts;

  const lookup = await loadSnapshotSkillLookup(env, snapshotDate, request);
  const entry = lookup?.entries?.[skillId];
  if (!entry) return null;

  const page = await loadSnapshotSkillsPage(env, snapshotDate, entry.page, request);
  const raw = page?.[entry.index];
  if (!raw) return null;

  const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
  const pageSize = snapshotManifest?.page_size ?? 200;
  const fetchedAt = `${snapshotDate}T00:00:00.000Z`;
  return {
    id: skillId,
    skill_id: raw.skillId,
    owner,
    repo,
    canonical_url: `https://skills.sh/${owner}/${repo}/${leafSkillId}`,
    total_installs: raw.installs ?? null,
    weekly_installs: null,
    weekly_installs_raw: null,
    platform_installs: null,
    name: raw.name,
    description: null,
    first_seen_date: null,
    github_url: `https://github.com/${owner}/${repo}`,
    og_image_url: null,
    run_id: `snapshot-${snapshotDate}`,
    fetched_at: fetchedAt,
    discovery_source: "leaderboard",
    source_endpoint: "leaderboard",
    discovery_pass: 1,
    rank_at_fetch: (entry.page - 1) * pageSize + entry.index + 1,
    http_status: 200,
    parser_version: "snapshot-proxy",
    raw_html_hash: null,
    skill_md_content: null,
    skill_md_frontmatter: null,
    install_command: null,
    categories: [],
  };
}

async function buildSnapshotMetricsResponse(
  env: Env,
  snapshotDate: string,
  skillId: string,
  request: Request,
): Promise<Record<string, unknown> | null> {
  const lookup = await loadSnapshotSkillLookup(env, snapshotDate, request);
  const entry = lookup?.entries?.[skillId];
  if (!entry) return null;
  const page = await loadSnapshotSkillsPage(env, snapshotDate, entry.page, request);
  const raw = page?.[entry.index];
  if (!raw) return null;
  return {
    id: skillId,
    items: [
      {
        id: skillId,
        snapshot_date: snapshotDate,
        total_installs: raw.installs ?? null,
        weekly_installs: null,
        platform_installs: null,
      },
    ],
  };
}

async function loadLatestManifest(env: Env, request: Request): Promise<LatestManifest | null> {
  const nowMs = Date.now();
  if (cachedLatestManifest && nowMs - cachedLatestManifest.loadedAtMs < IN_MEMORY_CACHE_TTL_MS) {
    return cachedLatestManifest.manifest;
  }

  let manifest: LatestManifest | null = null;

  const webManifestText = await loadJsonTextWithCache(env, LATEST_MANIFEST_KEY, request, "latest-manifest");
  if (webManifestText) {
    const parsed = parseJson<LatestManifest>(webManifestText, LATEST_MANIFEST_KEY);
    if (!parsed.snapshot_date || !isDateString(parsed.snapshot_date)) {
      throw new Error("latest.json missing valid snapshot_date");
    }
    manifest = parsed;
  } else {
    const snapshotLatestText = await loadJsonTextWithCache(env, LATEST_SNAPSHOT_KEY, request, "latest-snapshot");
    if (!snapshotLatestText) return null;
    const parsed = parseJson<SnapshotLatestManifest>(snapshotLatestText, LATEST_SNAPSHOT_KEY);
    if (!parsed.date || !isDateString(parsed.date)) {
      throw new Error("snapshots/latest.json missing valid date");
    }
    manifest = { snapshot_date: parsed.date };
    const snapshotManifest = await loadSnapshotSkillsManifest(env, parsed.date, request);
    if (snapshotManifest) {
      manifest.page_size = snapshotManifest.page_size;
      manifest.counts = {
        total_skills: snapshotManifest.total_skills,
      };
    }
  }

  cachedLatestManifest = { loadedAtMs: nowMs, manifest };
  return manifest;
}

async function loadSearchIndex(
  env: Env,
  snapshotDate: string,
  request: Request,
): Promise<SearchIndexDocument | null> {
  const key = SEARCH_INDEX_KEY(snapshotDate);
  const nowMs = Date.now();
  if (parsedSearchCache && parsedSearchCache.key === key && nowMs - parsedSearchCache.loadedAtMs < IN_MEMORY_CACHE_TTL_MS) {
    return parsedSearchCache.doc;
  }

  const text = await loadJsonTextWithCache(env, key, request, `search-index/${snapshotDate}`);
  if (!text) return null;
  const doc = parseJson<SearchIndexDocument>(text, key);
  if (!doc.snapshot_date || !isDateString(doc.snapshot_date) || !Array.isArray(doc.items)) {
    throw new Error("Invalid search index payload");
  }
  parsedSearchCache = { key, loadedAtMs: nowMs, doc };
  return doc;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: CORS_HEADERS });
      }

      if (request.method !== "GET") {
        return json({ error: "Method not allowed" }, { status: 405 });
      }

      const url = new URL(request.url);

      if (url.pathname.startsWith(`/${DATA_PREFIX}/`) && url.pathname.endsWith(".json")) {
        const key = url.pathname.slice(1);
        let text = await loadJsonTextWithCache(env, key, request, `static/${key}`);
        if (!text) {
          if (key === LATEST_MANIFEST_KEY) {
            const latest = await loadLatestManifest(env, request);
            if (latest) {
              const snapshotDate = latest.snapshot_date;
              const summary = await loadSnapshotSummary(env, snapshotDate, request);
              const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
              const pageSize = latest.page_size ?? snapshotManifest?.page_size ?? 200;
              text = JSON.stringify({
                format_version: 1,
                snapshot_date: snapshotDate,
                generated_at: new Date().toISOString(),
                page_size: pageSize,
                sort_modes: ["installs"],
                counts: {
                  total_skills:
                    summary?.total_skills ?? snapshotManifest?.total_skills ?? latest.counts?.total_skills ?? 0,
                  total_repos: summary?.total_repos ?? latest.counts?.total_repos ?? 0,
                },
                paths: {
                  stats_summary: `/${DATA_PREFIX}/snapshots/${snapshotDate}/stats/summary.json`,
                  leaderboard_page_template: `/${DATA_PREFIX}/snapshots/${snapshotDate}/leaderboard/{sort}/page-{page}.json`,
                  skill_detail_template: `/${DATA_PREFIX}/snapshots/${snapshotDate}/skills/by-id/{owner}/{repo}/{skill_id}.json`,
                  metrics_template: `/${DATA_PREFIX}/snapshots/${snapshotDate}/metrics/by-id/{owner}/{repo}/{skill_id}.json`,
                },
              });
            }
          } else {
            const statsMatch = key.match(
              new RegExp(`^${DATA_PREFIX}/snapshots/(\\d{4}-\\d{2}-\\d{2})/stats/summary\\.json$`),
            );
            if (statsMatch) {
              const [, snapshotDate] = statsMatch;
              const summary = await loadSnapshotSummary(env, snapshotDate, request);
              if (summary) {
                text = JSON.stringify(summary);
              }
            }
          }

          if (!text) {
            const leaderboardMatch = key.match(
              new RegExp(`^${DATA_PREFIX}/snapshots/(\\d{4}-\\d{2}-\\d{2})/leaderboard/([^/]+)/page-(\\d{4})\\.json$`),
            );
            if (leaderboardMatch) {
              const [, snapshotDate, sort, pageStr] = leaderboardMatch;
              if (sort === "installs") {
                const page = Number(pageStr);
                const snapshotPage = await loadSnapshotSkillsPage(env, snapshotDate, page, request);
                if (snapshotPage) {
                  const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
                  const summary = await loadSnapshotSummary(env, snapshotDate, request);
                  const pageSize = snapshotManifest?.page_size ?? 200;
                  const items = snapshotPage.map((item, index) =>
                    snapshotItemToSkillListItem(item, (page - 1) * pageSize + index + 1),
                  );
                  text = JSON.stringify({
                    snapshot_date: snapshotDate,
                    sort,
                    page,
                    page_size: pageSize,
                    total: summary?.total_skills ?? snapshotManifest?.total_skills ?? items.length,
                    items,
                  });
                }
              }
            }
          }

          if (!text) {
            const skillMatch = key.match(
              new RegExp(
                `^${DATA_PREFIX}/snapshots/(\\d{4}-\\d{2}-\\d{2})/skills/by-id/([^/]+)/([^/]+)/([^/]+)\\.json$`,
              ),
            );
            if (skillMatch) {
              const [, snapshotDate, ownerEnc, repoEnc, skillEnc] = skillMatch;
              let owner: string;
              let repo: string;
              let skillId: string;
              try {
                owner = decodeURIComponent(ownerEnc);
                repo = decodeURIComponent(repoEnc);
                skillId = decodeURIComponent(skillEnc);
              } catch {
                owner = "";
                repo = "";
                skillId = "";
              }
              if (owner && repo && skillId) {
                const record = await buildSnapshotSkillRecord(env, snapshotDate, `${owner}/${repo}/${skillId}`, request);
                if (record) {
                  text = JSON.stringify(record);
                }
              }
            }
          }

          if (!text) {
            const metricsMatch = key.match(
              new RegExp(
                `^${DATA_PREFIX}/snapshots/(\\d{4}-\\d{2}-\\d{2})/metrics/by-id/([^/]+)/([^/]+)/([^/]+)\\.json$`,
              ),
            );
            if (metricsMatch) {
              const [, snapshotDate, ownerEnc, repoEnc, skillEnc] = metricsMatch;
              let owner: string;
              let repo: string;
              let skillId: string;
              try {
                owner = decodeURIComponent(ownerEnc);
                repo = decodeURIComponent(repoEnc);
                skillId = decodeURIComponent(skillEnc);
              } catch {
                owner = "";
                repo = "";
                skillId = "";
              }
              if (owner && repo && skillId) {
                const metrics = await buildSnapshotMetricsResponse(env, snapshotDate, `${owner}/${repo}/${skillId}`, request);
                if (metrics) {
                  text = JSON.stringify(metrics);
                }
              }
            }
          }
        }
        if (!text) {
          return json({ error: "Not found" }, { status: 404 });
        }
        return new Response(text, {
          headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "public, max-age=60, s-maxage=60",
            "X-Content-Type-Options": "nosniff",
            ...CORS_HEADERS,
          },
        });
      }

      if (url.pathname === "/healthz") {
        let snapshotDate: string | null = null;
        try {
          const latest = await loadLatestManifest(env, request);
          snapshotDate = latest?.snapshot_date ?? null;
        } catch {
          // health endpoint stays best-effort
        }
        return json(
          {
            ok: true,
            search_index_loaded: parsedSearchCache !== null,
            snapshot_date: snapshotDate ?? parsedSearchCache?.doc.snapshot_date ?? null,
          },
          { headers: { "cache-control": "no-store" } },
        );
      }

      if (url.pathname === "/v1/stats/summary") {
        const { blocked, headers: rlHeaders } = enforceRateLimit(request);
        if (blocked) return blocked;

        const snapshotParam = url.searchParams.get("snapshot_date");
        let snapshotDate: string;
        if (snapshotParam) {
          if (!isDateString(snapshotParam)) {
            return json({ error: "Invalid snapshot_date format" }, { status: 400, headers: rlHeaders });
          }
          snapshotDate = snapshotParam;
        } else {
          const latest = await loadLatestManifest(env, request);
          if (!latest) {
            return json({ error: "Summary unavailable" }, { status: 503, headers: rlHeaders });
          }
          snapshotDate = latest.snapshot_date;
        }

        const summary =
          (await loadJsonObjectWithCache<Record<string, unknown>>(
            env,
            STATS_SUMMARY_KEY(snapshotDate),
            request,
            `summary/${snapshotDate}`,
          )) ??
          (await loadSnapshotSummary(env, snapshotDate, request));
        if (!summary) {
          const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
          if (!snapshotManifest) {
            return json({ error: "Summary unavailable" }, { status: 503, headers: rlHeaders });
          }
          return json(
            {
              total_skills: snapshotManifest.total_skills,
              total_repos: 0,
              snapshot_date: snapshotDate,
            },
            { headers: rlHeaders },
          );
        }
        return json(summary, { headers: rlHeaders });
      }

      if (url.pathname === "/v1/skills") {
        const { blocked, headers: rlHeaders } = enforceRateLimit(request);
        if (blocked) return blocked;

        const sortParam = url.searchParams.get("sort") ?? "installs";
        if (!isSortMode(sortParam)) {
          return json({ error: `Invalid sort field: ${sortParam}` }, { status: 400, headers: rlHeaders });
        }

        const page = Math.max(1, parsePositiveInt(url.searchParams.get("page"), 1));
        const pageSize = Math.min(200, Math.max(1, parsePositiveInt(url.searchParams.get("page_size"), 200)));
        const rawQ = (url.searchParams.get("q") ?? "").trim();

        const snapshotParam = url.searchParams.get("snapshot_date");
        let snapshotDate: string;
        let latestManifest: LatestManifest | null = null;
        if (snapshotParam) {
          if (!isDateString(snapshotParam)) {
            return json({ error: "Invalid snapshot_date format" }, { status: 400, headers: rlHeaders });
          }
          snapshotDate = snapshotParam;
        } else {
          latestManifest = await loadLatestManifest(env, request);
          if (!latestManifest) {
            return json({ error: "Leaderboard unavailable" }, { status: 503, headers: rlHeaders });
          }
          snapshotDate = latestManifest.snapshot_date;
        }

        if (rawQ.length === 0) {
          const cachedPage = await loadJsonObjectWithCache<Record<string, unknown>>(
            env,
            LEADERBOARD_PAGE_KEY(snapshotDate, sortParam, page),
            request,
            `leaderboard/${snapshotDate}/${sortParam}/${page}`,
          );
          if (cachedPage) {
            const payload = cachedPage as SearchResponse & { sort?: string };
            if (!("page_size" in payload) || payload.page_size === pageSize) {
              return json(payload, { headers: rlHeaders });
            }
          }

          if (sortParam === "installs") {
            const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
            const snapshotPage = await loadSnapshotSkillsPage(env, snapshotDate, page, request);
            const manifestPageSize = snapshotManifest?.page_size ?? 200;
            if (snapshotPage && pageSize === manifestPageSize) {
              const summary = await loadSnapshotSummary(env, snapshotDate, request);
              const totalSkills =
                summary?.total_skills ??
                snapshotManifest?.total_skills ??
                latestManifest?.counts?.total_skills ??
                snapshotPage.length;
              const items = snapshotPage.map((item, index) =>
                snapshotItemToSkillListItem(item, (page - 1) * manifestPageSize + index + 1),
              );
              return json(
                {
                  items,
                  skills: snapshotPage,
                  page,
                  page_size: pageSize,
                  total: totalSkills,
                  snapshot_date: snapshotDate,
                  sort: sortParam,
                },
                { headers: rlHeaders },
              );
            }
          }
        }

        const index = await loadSearchIndex(env, snapshotDate, request);
        if (!index) {
          return json({ error: "Leaderboard unavailable" }, { status: 503, headers: rlHeaders });
        }

        const query = rawQ.toLowerCase();
        const filtered = query
          ? index.items.filter((item) => {
              const name = item.name.toLowerCase();
              const id = item.id.toLowerCase();
              return name.includes(query) || id.includes(query);
            })
          : index.items;
        const sorted = sortItems(filtered, sortParam);
        const start = (page - 1) * pageSize;
        const payload: SearchResponse & { sort: SortMode } = {
          items: sorted.slice(start, start + pageSize),
          page,
          page_size: pageSize,
          total: sorted.length,
          snapshot_date: index.snapshot_date,
          sort: sortParam,
        };
        if (!query && payload.items.length === 0 && latestManifest?.counts?.total_skills) {
          payload.total = latestManifest.counts.total_skills;
        }
        return json(payload, { headers: { ...rlHeaders, "cache-control": "public, max-age=30, s-maxage=30" } });
      }

      if (url.pathname.startsWith("/v1/skills/")) {
        const { blocked, headers: rlHeaders } = enforceRateLimit(request);
        if (blocked) return blocked;

        const skillPath = parseSkillPath(url.pathname, "/v1/skills/");
        if (!skillPath) {
          return json({ error: "Invalid skill id" }, { status: 400, headers: rlHeaders });
        }

        const snapshotParam = url.searchParams.get("snapshot_date");
        let snapshotDate: string;
        if (snapshotParam) {
          if (!isDateString(snapshotParam)) {
            return json({ error: "Invalid snapshot_date format" }, { status: 400, headers: rlHeaders });
          }
          snapshotDate = snapshotParam;
        } else {
          const latest = await loadLatestManifest(env, request);
          if (!latest) {
            return json({ error: "Skill unavailable" }, { status: 503, headers: rlHeaders });
          }
          snapshotDate = latest.snapshot_date;
        }

        let record = await loadJsonObjectWithCache<Record<string, unknown>>(
          env,
          SKILL_DETAIL_KEY(snapshotDate, skillPath.owner, skillPath.repo, skillPath.skillId),
          request,
          `skill/${snapshotDate}/${skillPath.id}`,
        );
        if (!record) {
          record = await buildSnapshotSkillRecord(env, snapshotDate, skillPath.id, request);
          if (!record) {
            return json({ error: "Not found" }, { status: 404, headers: rlHeaders });
          }
        }
        return json(record, { headers: rlHeaders });
      }

      if (url.pathname.startsWith("/v1/metrics/")) {
        const { blocked, headers: rlHeaders } = enforceRateLimit(request);
        if (blocked) return blocked;

        const skillPath = parseSkillPath(url.pathname, "/v1/metrics/");
        if (!skillPath) {
          return json({ error: "Invalid skill id" }, { status: 400, headers: rlHeaders });
        }

        const snapshotParam = url.searchParams.get("snapshot_date");
        let snapshotDate: string;
        if (snapshotParam) {
          if (!isDateString(snapshotParam)) {
            return json({ error: "Invalid snapshot_date format" }, { status: 400, headers: rlHeaders });
          }
          snapshotDate = snapshotParam;
        } else {
          const latest = await loadLatestManifest(env, request);
          if (!latest) {
            return json({ error: "Metrics unavailable" }, { status: 503, headers: rlHeaders });
          }
          snapshotDate = latest.snapshot_date;
        }

        let metrics = await loadJsonObjectWithCache<Record<string, unknown>>(
          env,
          METRICS_KEY(snapshotDate, skillPath.owner, skillPath.repo, skillPath.skillId),
          request,
          `metrics/${snapshotDate}/${skillPath.id}`,
        );
        if (!metrics) {
          metrics = await buildSnapshotMetricsResponse(env, snapshotDate, skillPath.id, request);
          if (!metrics) {
            return json({ error: "Not found" }, { status: 404, headers: rlHeaders });
          }
        }
        return json(metrics, { headers: rlHeaders });
      }

      if (url.pathname === "/v1/search") {
        const { blocked, headers: rlHeaders } = enforceRateLimit(request);
        if (blocked) return blocked;

        const rawQ = (url.searchParams.get("q") ?? "").trim();
        if (rawQ.length < 2) {
          return json({ error: "q must be at least 2 characters" }, { status: 400, headers: rlHeaders });
        }
        if (rawQ.length > 100) {
          return json({ error: "q must be <= 100 characters" }, { status: 400, headers: rlHeaders });
        }

        const sortParam = url.searchParams.get("sort") ?? "installs";
        if (!isSortMode(sortParam)) {
          return json({ error: `Invalid sort field: ${sortParam}` }, { status: 400, headers: rlHeaders });
        }

        const page = Math.max(1, parsePositiveInt(url.searchParams.get("page"), 1));
        const pageSize = Math.min(50, Math.max(1, parsePositiveInt(url.searchParams.get("page_size"), 12)));

        const snapshotParam = url.searchParams.get("snapshot_date");
        let snapshotDate: string;
        if (snapshotParam) {
          if (!isDateString(snapshotParam)) {
            return json({ error: "Invalid snapshot_date format" }, { status: 400, headers: rlHeaders });
          }
          snapshotDate = snapshotParam;
        } else {
          const latest = await loadLatestManifest(env, request);
          if (!latest) {
            return json({ error: "Search index unavailable" }, { status: 503, headers: rlHeaders });
          }
          snapshotDate = latest.snapshot_date;
        }

        const index = await loadSearchIndex(env, snapshotDate, request);
        const query = rawQ.toLowerCase();
        if (index) {
          const filtered = index.items.filter((item) => {
            const name = item.name.toLowerCase();
            const id = item.id.toLowerCase();
            return name.includes(query) || id.includes(query);
          });
          const sorted = sortItems(filtered, sortParam);
          const start = (page - 1) * pageSize;
          const payload: SearchResponse = {
            items: sorted.slice(start, start + pageSize),
            page,
            page_size: pageSize,
            total: sorted.length,
            snapshot_date: index.snapshot_date,
          };
          return json(payload, { headers: { ...rlHeaders, "cache-control": "public, max-age=30, s-maxage=30" } });
        }

        const lookup = await loadSnapshotSkillLookup(env, snapshotDate, request);
        if (!lookup) {
          return json({ error: "Search index unavailable" }, { status: 503, headers: rlHeaders });
        }
        const entries = Object.entries(lookup.entries).filter(([id]) => id.toLowerCase().includes(query));
        const sortedEntries =
          sortParam === "name"
            ? entries.sort((a, b) => a[0].localeCompare(b[0]))
            : entries.sort((a, b) => a[1].page - b[1].page || a[1].index - b[1].index);

        const start = (page - 1) * pageSize;
        const selected = sortedEntries.slice(start, start + pageSize);
        const snapshotManifest = await loadSnapshotSkillsManifest(env, snapshotDate, request);
        const rawPageSize = snapshotManifest?.page_size ?? 200;
        const pageCache = new Map<number, SnapshotSkillPageItem[]>();
        const items: SkillListItem[] = [];
        for (const [id, ref] of selected) {
          let snapshotPage = pageCache.get(ref.page);
          if (!snapshotPage) {
            snapshotPage = (await loadSnapshotSkillsPage(env, snapshotDate, ref.page, request)) ?? [];
            pageCache.set(ref.page, snapshotPage);
          }
          const raw = snapshotPage[ref.index];
          if (!raw) continue;
          const mapped = snapshotItemToSkillListItem(raw, (ref.page - 1) * rawPageSize + ref.index + 1);
          if (mapped.id !== id) {
            mapped.id = id;
          }
          items.push(mapped);
        }

        const fallbackPayload: SearchResponse = {
          items,
          page,
          page_size: pageSize,
          total: sortedEntries.length,
          snapshot_date: snapshotDate,
        };
        return json(fallbackPayload, { headers: { ...rlHeaders, "cache-control": "public, max-age=30, s-maxage=30" } });
      }

      return json({ error: "Not found" }, { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal Server Error";
      return json({ error: message || "Internal Server Error" }, { status: 500 });
    }
  },
};
