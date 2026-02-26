import { describe, expect, it } from "vitest";

import { reconcileSelectedId } from "@/hooks/useLeaderboardData";

describe("reconcileSelectedId", () => {
  const skills = [{ id: "alpha" }, { id: "beta" }];

  it("selects the first skill when nothing is selected", () => {
    expect(reconcileSelectedId(null, skills)).toBe("alpha");
  });

  it("keeps the selection when it is still present", () => {
    expect(reconcileSelectedId("beta", skills)).toBe("beta");
  });

  it("resets stale selection to the first available skill", () => {
    expect(reconcileSelectedId("missing", skills)).toBe("alpha");
  });

  it("clears selection when no skills are available", () => {
    expect(reconcileSelectedId("alpha", [])).toBeNull();
  });
});
