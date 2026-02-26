import { describe, expect, it } from "vitest";

import { formatInstallCount, getPlatformInstallEntries, getSkillDescription } from "@/lib/skillPresentation";

describe("skillPresentation", () => {
  it("formats counts and falls back to zero", () => {
    expect(formatInstallCount(12345)).toBe("12,345");
    expect(formatInstallCount(null)).toBe("0");
  });

  it("returns fallback description when missing", () => {
    expect(getSkillDescription("Visible")).toBe("Visible");
    expect(getSkillDescription(undefined)).toBe("No description available.");
    expect(getSkillDescription(null)).toBe("No description available.");
  });

  it("keeps only positive platform installs", () => {
    expect(
      getPlatformInstallEntries({
        codex: 10,
        amp: 0,
        github_copilot: null,
        opencode: 3,
      }),
    ).toEqual([
      { platform: "codex", count: 10 },
      { platform: "opencode", count: 3 },
    ]);
  });
});
