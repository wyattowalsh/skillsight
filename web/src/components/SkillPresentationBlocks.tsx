import type { ReactNode } from "react";

import type { SkillRecord } from "@/contracts/types";
import { formatInstallCount, getSkillDescription } from "@/lib/skillPresentation";
import { Badge } from "@/components/ui/badge";
import { CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

type SkillSummaryBlockProps = {
  skill: SkillRecord;
  titleClassName?: string;
  descriptionClassName?: string;
  layout?: "stacked" | "split";
  trailing?: ReactNode;
};

export function SkillSummaryBlock({
  skill,
  titleClassName,
  descriptionClassName,
  layout = "stacked",
  trailing,
}: SkillSummaryBlockProps) {
  const summaryContent = (
    <>
      <CardTitle className={titleClassName}>{skill.name}</CardTitle>
      <p className={cn("text-sm text-muted-foreground", descriptionClassName)}>
        {getSkillDescription(skill.description)}
      </p>
    </>
  );

  if (layout === "split") {
    return (
      <div className="flex items-start justify-between">
        <div>{summaryContent}</div>
        {trailing}
      </div>
    );
  }

  return summaryContent;
}

type PlatformEntry = {
  platform: string;
  count: number;
};

type SkillPlatformListProps = {
  platforms: PlatformEntry[];
  variant?: "badges" | "rows";
};

export function SkillPlatformList({ platforms, variant = "badges" }: SkillPlatformListProps) {
  if (variant === "rows") {
    return (
      <>
        {platforms.map(({ platform, count }) => (
          <div key={platform} className="flex items-center justify-between">
            <Badge variant="outline" className="font-mono text-xs">
              {platform}
            </Badge>
            <span className="font-mono text-sm tabular-nums">{formatInstallCount(count)}</span>
          </div>
        ))}
      </>
    );
  }

  return (
    <div className="flex flex-wrap gap-1.5">
      {platforms.map(({ platform, count }) => (
        <Badge key={platform} variant="outline" className="font-mono text-xs">
          {platform}: {formatInstallCount(count)}
        </Badge>
      ))}
    </div>
  );
}
