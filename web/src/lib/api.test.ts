import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type FetchResponseInit = {
  status?: number;
  body?: unknown;
};

function jsonResponse(init: FetchResponseInit = {}): Response {
  const status = init.status ?? 200;
  const body =
    init.body === undefined ? "{}" : typeof init.body === "string" ? init.body : JSON.stringify(init.body);
  return new Response(body, {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("api client runtime caching", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("retries manifest fetch after a transient latest.json failure", async () => {
    let latestCalls = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        latestCalls += 1;
        if (latestCalls === 1) {
          throw new Error("network down");
        }
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
          },
        });
      }
      if (url.includes("/stats/summary.json")) {
        return jsonResponse({
          body: { total_skills: 1, total_repos: 1, snapshot_date: "2025-01-15" },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");

    await expect(api.getStatsSummary()).rejects.toThrow("network down");
    await expect(api.getStatsSummary()).resolves.toEqual({
      total_skills: 1,
      total_repos: 1,
      snapshot_date: "2025-01-15",
    });
    expect(latestCalls).toBe(2);
  });

  it("retries slim-index fetch after a transient search index failure", async () => {
    let slimIndexCalls = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
          },
        });
      }
      if (url.includes("/search/slim-index.json")) {
        slimIndexCalls += 1;
        if (slimIndexCalls === 1) {
          throw new Error("transient search index failure");
        }
        return jsonResponse({
          body: {
            snapshot_date: "2025-01-15",
            items: [
              {
                id: "o/r/alpha",
                skill_id: "alpha",
                owner: "o",
                repo: "r",
                canonical_url: "https://skills.sh/o/r/alpha",
                name: "Alpha",
                total_installs: 10,
                weekly_installs: 2,
                rank_at_fetch: 1,
              },
            ],
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");

    await expect(api.listSkills(1, 12, "alp", "installs")).rejects.toThrow("transient search index failure");
    const second = await api.listSkills(1, 12, "alp", "installs");
    expect(second.total).toBe(1);
    expect(second.items[0]?.id).toBe("o/r/alpha");
    expect(slimIndexCalls).toBe(2);
  });

  it("evicts failed detail requests from the request cache", async () => {
    let detailCalls = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
          },
        });
      }
      if (url.includes("/skills/by-id/o/r/a.json")) {
        detailCalls += 1;
        if (detailCalls === 1) {
          return jsonResponse({ status: 500, body: { error: "boom" } });
        }
        return jsonResponse({
          body: {
            id: "o/r/a",
            skill_id: "a",
            owner: "o",
            repo: "r",
            canonical_url: "https://skills.sh/o/r/a",
            name: "Alpha",
            run_id: "run-1",
            fetched_at: "2025-01-15T00:00:00Z",
            discovery_source: "search_api",
            source_endpoint: "search_api",
            discovery_pass: 1,
            parser_version: "0.1.0",
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");

    await expect(api.getSkill("o/r/a")).rejects.toThrow("Request failed: 500");
    const skill = await api.getSkill("o/r/a");
    expect(skill.id).toBe("o/r/a");
    expect(detailCalls).toBe(2);
  });

  it("does not silently substitute page 1 when a later static page fails", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
          },
        });
      }
      if (url.includes("/leaderboard/installs/page-0002.json")) {
        return jsonResponse({ status: 404, body: { error: "missing page" } });
      }
      if (url.includes("/leaderboard/installs/page-0001.json")) {
        return jsonResponse({
          body: { items: [{ id: "unexpected" }], page: 1, page_size: 12, total: 1 },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");

    await expect(api.listSkills(2, 12, "", "installs")).rejects.toThrow("Request failed: 404");
    expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining("page-0001.json"));
  });

  it("supports local search fallback with slim search records", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
          },
        });
      }
      if (url.includes("/search/slim-index.json")) {
        return jsonResponse({
          body: {
            snapshot_date: "2025-01-15",
            items: [
              {
                id: "o/r/alpha",
                skill_id: "alpha",
                owner: "o",
                repo: "r",
                canonical_url: "https://skills.sh/o/r/alpha",
                name: "Alpha Tool",
                total_installs: 50,
                weekly_installs: 5,
                rank_at_fetch: 2,
              },
              {
                id: "o/r/beta",
                skill_id: "beta",
                owner: "o",
                repo: "r",
                canonical_url: "https://skills.sh/o/r/beta",
                name: "Beta Tool",
                total_installs: 100,
                weekly_installs: 1,
                rank_at_fetch: 1,
              },
            ],
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");
    const result = await api.listSkills(1, 12, "tool", "installs");

    expect(result.total).toBe(2);
    expect(result.items.map((item) => item.id)).toEqual(["o/r/beta", "o/r/alpha"]);
    expect(result.items[0]).not.toHaveProperty("description");
    expect(result.items[0]).not.toHaveProperty("platform_installs");
  });

  it("loads wave-a analytics using manifest path overrides", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
            paths: {
              stats_wave_a: "/custom/wave-a.json",
            },
          },
        });
      }
      if (url.includes("/custom/wave-a.json")) {
        return jsonResponse({
          body: {
            snapshot_date: "2025-01-15",
            previous_snapshot_date: "2025-01-14",
            rank_turbulence: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: "2025-01-14",
              kpis: {
                matched_skill_count: 0,
                median_abs_rank_delta: null,
                p90_abs_rank_delta: null,
                improved_count: 0,
                declined_count: 0,
                unchanged_count: 0,
              },
              buckets: [],
              top_gainers: [],
              top_losers: [],
            },
            momentum_vs_scale: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: "2025-01-14",
              kpis: {
                positive_momentum_pct: null,
                median_delta_installs: null,
                p90_delta_installs: null,
              },
              points: [],
            },
            long_tail_power_curve: {
              snapshot_date: "2025-01-15",
              kpis: {
                total_installs_sum: 0,
                top10_share_pct: 0,
                top50_share_pct: 0,
                top100_share_pct: 0,
              },
              curve: [],
            },
            source_effectiveness: {
              snapshot_date: "2025-01-15",
              kpis: {
                source_count: 0,
                dominant_source: null,
                dominant_source_share_pct: null,
              },
              sources: [],
            },
            daily_change_cards: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: "2025-01-14",
              compared_skill_count: 0,
              cards: [],
            },
            limitations: [],
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");
    const analytics = await api.getWaveAAnalytics();

    expect(analytics.snapshot_date).toBe("2025-01-15");
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/custom/wave-a.json"));
  });

  it("loads enrichment stats using manifest path overrides", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
            paths: {
              stats_enrichment: "/custom/enrichment.json",
            },
          },
        });
      }
      if (url.includes("/custom/enrichment.json")) {
        return jsonResponse({
          body: {
            snapshot_date: "2025-01-15",
            total_skills: 100,
            history_mode: "previous_snapshot",
            history_snapshots_considered: 1,
            backfilled: {
              platform_installs: 3,
              categories: 4,
              first_seen_date: 5,
            },
            coverage: {
              platform_installs: { count: 20, pct: 20 },
              categories: { count: 50, pct: 50 },
              first_seen_date: { count: 80, pct: 80 },
            },
            ready: {
              platform_installs: true,
              categories: true,
              first_seen_date: true,
              wave_b: true,
            },
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");
    const enrichment = await api.getEnrichmentStats();

    expect(enrichment.total_skills).toBe(100);
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/custom/enrichment.json"));
  });

  it("normalizes manifest paths that already include /data/v1", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/data/v1/latest.json")) {
        return jsonResponse({
          body: {
            format_version: 1,
            snapshot_date: "2025-01-15",
            generated_at: "2025-01-15T00:00:00Z",
            page_size: 12,
            sort_modes: ["installs", "weekly", "name"],
            paths: {
              stats_wave_a: "/data/v1/snapshots/2025-01-15/stats/wave-a.json",
            },
          },
        });
      }
      if (url.includes("/data/v1/snapshots/2025-01-15/stats/wave-a.json")) {
        if (url.includes("/data/v1/data/v1/")) {
          throw new Error(`Double-prefixed path ${url}`);
        }
        return jsonResponse({
          body: {
            snapshot_date: "2025-01-15",
            previous_snapshot_date: null,
            rank_turbulence: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: null,
              kpis: {
                matched_skill_count: 0,
                median_abs_rank_delta: null,
                p90_abs_rank_delta: null,
                improved_count: 0,
                declined_count: 0,
                unchanged_count: 0,
              },
              buckets: [],
              top_gainers: [],
              top_losers: [],
            },
            momentum_vs_scale: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: null,
              kpis: {
                positive_momentum_pct: null,
                median_delta_installs: null,
                p90_delta_installs: null,
              },
              points: [],
            },
            long_tail_power_curve: {
              snapshot_date: "2025-01-15",
              kpis: {
                total_installs_sum: 0,
                top10_share_pct: 0,
                top50_share_pct: 0,
                top100_share_pct: 0,
              },
              curve: [],
            },
            source_effectiveness: {
              snapshot_date: "2025-01-15",
              kpis: {
                source_count: 0,
                dominant_source: null,
                dominant_source_share_pct: null,
              },
              sources: [],
            },
            daily_change_cards: {
              snapshot_date: "2025-01-15",
              previous_snapshot_date: null,
              compared_skill_count: 0,
              cards: [],
            },
            limitations: [],
          },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const api = await import("./api");
    const analytics = await api.getWaveAAnalytics();

    expect(analytics.snapshot_date).toBe("2025-01-15");
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/data/v1/snapshots/2025-01-15/stats/wave-a.json"),
    );
    expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining("/data/v1/data/v1/"));
  });
});
