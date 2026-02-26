/**
 * Types derived from contracts/worker_openapi.json
 * Keep in sync with OpenAPI spec — validated by Python tests in tests/unit/test_contract_sync.py
 * TODO: Auto-generate from OpenAPI spec (e.g., openapi-typescript)
 */

export type PlatformInstalls = {
  opencode?: number | null;
  codex?: number | null;
  gemini_cli?: number | null;
  github_copilot?: number | null;
  amp?: number | null;
  kimi_cli?: number | null;
  [key: string]: number | null | undefined;
};

export type SkillRecord = {
  id: string;
  skill_id: string;
  owner: string;
  repo: string;
  canonical_url: string;
  total_installs?: number | null;
  weekly_installs?: number | null;
  weekly_installs_raw?: string | null;
  platform_installs?: PlatformInstalls | null;
  name: string;
  description?: string | null;
  first_seen_date?: string | null;
  github_url?: string | null;
  og_image_url?: string | null;
  run_id: string;
  fetched_at: string;
  discovery_source: string;
  source_endpoint: string;
  discovery_pass: number;
  rank_at_fetch?: number | null;
  http_status?: number | null;
  parser_version: string;
  raw_html_hash?: string | null;
  skill_md_content?: string | null;
  skill_md_frontmatter?: Record<string, unknown> | null;
  install_command?: string | null;
  categories?: string[];
};

export type SkillListItem = {
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
