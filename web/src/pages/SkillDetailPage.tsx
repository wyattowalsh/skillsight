import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ArrowLeft, Copy, ExternalLink, Check } from "lucide-react";

import type { SkillRecord } from "@/contracts/types";
import type { MetricItem } from "@/lib/api";
import { getMetrics, getSkill } from "@/lib/api";
import { formatInstallCount, getPlatformInstallEntries } from "@/lib/skillPresentation";
import MetricChart from "@/components/MetricChart";
import { SkillPlatformList, SkillSummaryBlock } from "@/components/SkillPresentationBlocks";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      console.warn("Clipboard write failed — browser may require HTTPS or focus");
    }
  };

  return (
    <Button variant="ghost" size="icon-xs" onClick={handleCopy} aria-label="Copy install command">
      {copied ? <Check className="size-3" /> : <Copy className="size-3" />}
    </Button>
  );
}

export default function SkillDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [skill, setSkill] = useState<SkillRecord | null>(null);
  const [metrics, setMetrics] = useState<MetricItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    const decoded = decodeURIComponent(id);
    setLoading(true);
    setError(null);

    getSkill(decoded)
      .then(setSkill)
      .catch((err) => {
        setSkill(null);
        setError(err instanceof Error ? err.message : "Failed to load skill");
      })
      .finally(() => setLoading(false));

    getMetrics(decoded)
      .then((r) => setMetrics(r.items))
      .catch(() => setMetrics([]));
  }, [id]);

  if (loading) {
    return (
      <div className="flex h-64 items-center justify-center text-muted-foreground">
        Loading skill...
      </div>
    );
  }

  if (error || !skill) {
    return (
      <div className="flex h-64 flex-col items-center justify-center gap-2">
        <p className="text-destructive">{error ?? "Skill not found"}</p>
        <Button variant="outline" size="sm" asChild>
          <Link to="/">Back to leaderboard</Link>
        </Button>
      </div>
    );
  }

  const installCommand = `npx skills add ${skill.canonical_url}`;

  const platforms = getPlatformInstallEntries(skill.platform_installs);

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" asChild>
          <Link to="/">
            <ArrowLeft className="size-4" />
            Back
          </Link>
        </Button>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1fr_380px]">
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <SkillSummaryBlock
                skill={skill}
                titleClassName="text-xl"
                descriptionClassName="mt-1"
                layout="split"
                trailing={skill.rank_at_fetch ? (
                  <Badge variant="secondary" className="font-mono">
                    #{skill.rank_at_fetch}
                  </Badge>
                ) : undefined}
              />
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center gap-2 rounded-lg border bg-muted/50 px-3 py-2">
                <code className="flex-1 font-mono text-sm">{installCommand}</code>
                <CopyButton text={installCommand} />
              </div>

              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Total Installs
                  </p>
                  <p className="mt-1 font-mono text-lg font-semibold">
                    {formatInstallCount(skill.total_installs)}
                  </p>
                </div>
                <div>
                  <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Weekly
                  </p>
                  <p className="mt-1 font-mono text-lg font-semibold">
                    {formatInstallCount(skill.weekly_installs)}
                  </p>
                </div>
                <div>
                  <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Owner
                  </p>
                  <p className="mt-1 font-mono text-sm">{skill.owner}</p>
                </div>
                <div>
                  <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
                    Repo
                  </p>
                  <p className="mt-1 font-mono text-sm">{skill.repo}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <MetricChart items={metrics} />
        </div>

        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">Metadata</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">ID</span>
                <span className="font-mono text-xs">{skill.id}</span>
              </div>
              <Separator />
              <div className="flex justify-between">
                <span className="text-muted-foreground">Skill ID</span>
                <span className="font-mono text-xs">{skill.skill_id}</span>
              </div>
              <Separator />
              <div className="flex justify-between">
                <span className="text-muted-foreground">First Seen</span>
                <span className="font-mono text-xs">{skill.first_seen_date ?? "--"}</span>
              </div>
              <Separator />
              <div className="flex justify-between">
                <span className="text-muted-foreground">Discovery</span>
                <span className="font-mono text-xs">{skill.discovery_source}</span>
              </div>
              <Separator />
              <div className="flex justify-between">
                <span className="text-muted-foreground">Fetched At</span>
                <span className="font-mono text-xs">{new Date(skill.fetched_at).toLocaleDateString()}</span>
              </div>
            </CardContent>
          </Card>

          {platforms.length > 0 && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium">Platform Breakdown</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <SkillPlatformList platforms={platforms} variant="rows" />
              </CardContent>
            </Card>
          )}

          <div className="flex flex-col gap-2">
            {skill.github_url && (
              <Button variant="outline" size="sm" className="w-full" asChild>
                <a href={skill.github_url} target="_blank" rel="noopener noreferrer">
                  <ExternalLink className="size-4" />
                  View on GitHub
                </a>
              </Button>
            )}
            <Button variant="outline" size="sm" className="w-full" asChild>
              <a href={skill.canonical_url} target="_blank" rel="noopener noreferrer">
                <ExternalLink className="size-4" />
                View on skills.sh
              </a>
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
