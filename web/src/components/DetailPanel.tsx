import { ExternalLink } from "lucide-react";

import type { SkillRecord } from "@/contracts/types";
import type { MetricItem } from "@/lib/api";
import { formatInstallCount, getPlatformInstallEntries } from "@/lib/skillPresentation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { SkillPlatformList, SkillSummaryBlock } from "@/components/SkillPresentationBlocks";
import { Separator } from "@/components/ui/separator";
import MetricChart from "@/components/MetricChart";

type DetailPanelProps = {
  skill: SkillRecord | null;
  metrics: MetricItem[];
};

export default function DetailPanel({ skill, metrics }: DetailPanelProps) {
  if (!skill) {
    return (
      <Card className="flex min-h-[420px] items-center justify-center">
        <CardContent className="text-center">
          <p className="text-lg font-medium text-muted-foreground">No Skill Selected</p>
          <p className="mt-1 text-sm text-muted-foreground">
            Choose a skill from the table to inspect detail and trend data.
          </p>
        </CardContent>
      </Card>
    );
  }

  const platforms = getPlatformInstallEntries(skill.platform_installs);

  return (
    <Card>
      <CardHeader className="pb-3">
        <SkillSummaryBlock skill={skill} titleClassName="text-lg" />
      </CardHeader>

      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              ID
            </p>
            <p className="mt-0.5 font-mono text-sm">{skill.id}</p>
          </div>
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Repo
            </p>
            <p className="mt-0.5 font-mono text-sm">{skill.owner}/{skill.repo}</p>
          </div>
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Total Installs
            </p>
            <p className="mt-0.5 font-mono text-sm font-semibold">
              {formatInstallCount(skill.total_installs)}
            </p>
          </div>
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Weekly Installs
            </p>
            <p className="mt-0.5 font-mono text-sm font-semibold">
              {formatInstallCount(skill.weekly_installs)}
            </p>
          </div>
        </div>

        {platforms.length > 0 && (
          <>
            <Separator />
            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Platforms
              </p>
              <SkillPlatformList platforms={platforms} />
            </div>
          </>
        )}

        <Separator />

        <MetricChart items={metrics} />

        {skill.github_url && (
          <Button variant="outline" size="sm" className="w-full" asChild>
            <a href={skill.github_url} target="_blank" rel="noopener noreferrer">
              <ExternalLink className="size-4" />
              View on GitHub
            </a>
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
