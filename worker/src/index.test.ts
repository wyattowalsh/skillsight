import { beforeEach, describe, expect, it, vi } from "vitest";

import worker, { resetWorkerStateForTests } from "./index";

type CacheLike = {
  match: (request: Request) => Promise<Response | undefined>;
  put: (request: Request, response: Response) => Promise<void>;
};

type FakeBucket = {
  get: (key: string) => Promise<{ text: () => Promise<string> } | null>;
};

function installCacheMock(): CacheLike {
  const store = new Map<string, Response>();
  const cache: CacheLike = {
    async match(request: Request) {
      const cached = store.get(request.url);
      return cached ? cached.clone() : undefined;
    },
    async put(request: Request, response: Response) {
      store.set(request.url, response.clone());
    },
  };
  Object.defineProperty(globalThis, "caches", {
    configurable: true,
    value: { default: cache },
  });
  return cache;
}

function makeBucket(objects: Record<string, string>): FakeBucket {
  return {
    async get(key: string) {
      const text = objects[key];
      if (text === undefined) return null;
      return {
        text: async () => text,
      };
    },
  };
}

async function call(path: string, init: RequestInit = {}, env?: { SKILLSIGHT_DATA?: FakeBucket }): Promise<Response> {
  const request = new Request(`https://worker.test${path}`, init);
  return worker.fetch(request, (env ?? {}) as never);
}

async function jsonBody(response: Response): Promise<unknown> {
  return response.json();
}

function latestManifest(snapshotDate = "2025-01-15"): string {
  return JSON.stringify({ snapshot_date: snapshotDate });
}

function slimIndex(payload?: unknown): string {
  return JSON.stringify(
    payload ?? {
      snapshot_date: "2025-01-15",
      items: [
        {
          id: "o/r/alpha",
          skill_id: "alpha",
          owner: "o",
          repo: "r",
          name: "Alpha Tool",
          canonical_url: "https://skills.sh/o/r/alpha",
          total_installs: 100,
          weekly_installs: 10,
          rank_at_fetch: 1,
        },
        {
          id: "o/r/beta",
          skill_id: "beta",
          owner: "o",
          repo: "r",
          name: "Beta Tool",
          canonical_url: "https://skills.sh/o/r/beta",
          total_installs: 50,
          weekly_installs: 20,
          rank_at_fetch: 2,
        },
      ],
    },
  );
}

describe("tiny search worker", () => {
  beforeEach(() => {
    installCacheMock();
    resetWorkerStateForTests();
  });

  it("returns best-effort healthz without an R2 binding", async () => {
    const response = await call("/healthz");
    expect(response.status).toBe(200);
    expect(await jsonBody(response)).toEqual({
      ok: true,
      search_index_loaded: false,
      snapshot_date: null,
    });
  });

  it("returns paginated search results on the happy path", async () => {
    const env = {
      SKILLSIGHT_DATA: makeBucket({
        "data/v1/latest.json": latestManifest(),
        "data/v1/snapshots/2025-01-15/search/slim-index.json": slimIndex(),
      }),
    };

    const response = await call("/v1/search?q=tool&page=1&page_size=1&sort=weekly", {}, env);
    expect(response.status).toBe(200);
    expect(response.headers.get("X-RateLimit-Limit")).toBe("60");
    const body = (await jsonBody(response)) as {
      items: Array<{ id: string }>;
      page: number;
      page_size: number;
      total: number;
      snapshot_date: string;
    };
    expect(body.page).toBe(1);
    expect(body.page_size).toBe(1);
    expect(body.total).toBe(2);
    expect(body.snapshot_date).toBe("2025-01-15");
    expect(body.items[0]?.id).toBe("o/r/beta");
  });

  it("rejects too-short queries", async () => {
    const response = await call("/v1/search?q=a");
    expect(response.status).toBe(400);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "q must be at least 2 characters" });
  });

  it("rejects invalid sort values", async () => {
    const response = await call("/v1/search?q=tool&sort=bad");
    expect(response.status).toBe(400);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Invalid sort field: bad" });
  });

  it("rejects invalid snapshot_date format", async () => {
    const response = await call("/v1/search?q=tool&snapshot_date=2025-13-40");
    expect(response.status).toBe(400);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Invalid snapshot_date format" });
  });

  it("returns 503 when latest manifest is unavailable", async () => {
    const env = { SKILLSIGHT_DATA: makeBucket({}) };
    const response = await call("/v1/search?q=tool", {}, env);
    expect(response.status).toBe(503);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Search index unavailable" });
  });

  it("returns 503 when the slim search index is unavailable", async () => {
    const env = {
      SKILLSIGHT_DATA: makeBucket({
        "data/v1/latest.json": latestManifest(),
      }),
    };
    const response = await call("/v1/search?q=tool", {}, env);
    expect(response.status).toBe(503);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Search index unavailable" });
  });

  it("returns 429 with retry and rate-limit headers after limit is exceeded", async () => {
    const env = {
      SKILLSIGHT_DATA: makeBucket({
        "data/v1/latest.json": latestManifest(),
        "data/v1/snapshots/2025-01-15/search/slim-index.json": slimIndex(),
      }),
    };

    for (let i = 0; i < 60; i += 1) {
      const ok = await call("/v1/search?q=tool", { headers: { "CF-Connecting-IP": "203.0.113.7" } }, env);
      expect(ok.status).toBe(200);
    }

    const blocked = await call("/v1/search?q=tool", { headers: { "CF-Connecting-IP": "203.0.113.7" } }, env);
    expect(blocked.status).toBe(429);
    expect(blocked.headers.get("Retry-After")).toBeTruthy();
    expect(blocked.headers.get("X-RateLimit-Limit")).toBe("60");
    expect(blocked.headers.get("X-RateLimit-Remaining")).toBe("0");
    await expect(jsonBody(blocked)).resolves.toMatchObject({ error: "Too Many Requests" });
  });

  it("returns 405 for non-GET methods", async () => {
    const response = await call("/v1/search?q=tool", { method: "POST" });
    expect(response.status).toBe(405);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Method not allowed" });
  });

  it("returns 404 for unknown paths", async () => {
    const response = await call("/missing");
    expect(response.status).toBe(404);
    await expect(jsonBody(response)).resolves.toMatchObject({ error: "Not found" });
  });

  it("returns 500 when latest manifest or search index JSON is malformed", async () => {
    const envBadManifest = {
      SKILLSIGHT_DATA: makeBucket({
        "data/v1/latest.json": "{not-json",
      }),
    };
    const badManifest = await call("/v1/search?q=tool", {}, envBadManifest);
    expect(badManifest.status).toBe(500);

    resetWorkerStateForTests();
    installCacheMock();

    const envBadIndex = {
      SKILLSIGHT_DATA: makeBucket({
        "data/v1/latest.json": latestManifest(),
        "data/v1/snapshots/2025-01-15/search/slim-index.json": "{not-json",
      }),
    };
    const badIndex = await call("/v1/search?q=tool", {}, envBadIndex);
    expect(badIndex.status).toBe(500);
  });
});
