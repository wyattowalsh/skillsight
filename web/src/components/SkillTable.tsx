import { useNavigate } from "react-router-dom";

import type { SkillListItem } from "@/contracts/types";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

type SkillTableProps = {
  skills: SkillListItem[];
  selectedId: string | null;
  onSelect: (id: string) => void;
};

export default function SkillTable({ skills, selectedId, onSelect }: SkillTableProps) {
  const navigate = useNavigate();

  return (
    <Table>
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead className="w-[30%]">Skill</TableHead>
          <TableHead>Owner</TableHead>
          <TableHead className="text-right">Total Installs</TableHead>
          <TableHead className="text-right">Weekly</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {skills.length === 0 && (
          <TableRow>
            <TableCell colSpan={4} className="h-32 text-center text-muted-foreground">
              No skills found.
            </TableCell>
          </TableRow>
        )}
        {skills.map((skill) => (
          <TableRow
            key={skill.id}
            role="button"
            tabIndex={0}
            className={cn(
              "cursor-pointer transition-colors",
              selectedId === skill.id && "bg-primary/5",
            )}
            onClick={() => onSelect(skill.id)}
            onDoubleClick={() => navigate(`/skill/${encodeURIComponent(skill.id)}`)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onSelect(skill.id);
              }
            }}
          >
            <TableCell>
              <div className="flex flex-col gap-0.5">
                <span className="font-medium">{skill.name}</span>
                <span className="font-mono text-xs text-muted-foreground">{skill.skill_id}</span>
              </div>
            </TableCell>
            <TableCell>
              <Badge variant="secondary" className="font-mono text-xs">
                {skill.owner}
              </Badge>
            </TableCell>
            <TableCell className="text-right font-mono tabular-nums">
              {(skill.total_installs ?? 0).toLocaleString()}
            </TableCell>
            <TableCell className="text-right font-mono tabular-nums">
              {(skill.weekly_installs ?? 0).toLocaleString()}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
