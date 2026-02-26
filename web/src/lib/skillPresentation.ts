import type { PlatformInstalls } from "@/contracts/types";

const EMPTY_DESCRIPTION = "No description available.";

export function formatInstallCount(count?: number | null): string {
  return (count ?? 0).toLocaleString();
}

export function getSkillDescription(description?: string | null): string {
  return description ?? EMPTY_DESCRIPTION;
}

export function getPlatformInstallEntries(
  platformInstalls?: PlatformInstalls | null,
): Array<{ platform: string; count: number }> {
  if (!platformInstalls) {
    return [];
  }

  return Object.entries(platformInstalls)
    .filter((entry): entry is [string, number] => {
      const value = entry[1];
      return typeof value === "number" && value > 0;
    })
    .map(([platform, count]) => ({ platform, count }));
}
