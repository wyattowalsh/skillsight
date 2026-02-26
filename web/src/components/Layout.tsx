import { Link, NavLink, Outlet } from "react-router-dom";
import { BarChart3, Home, TrendingUp } from "lucide-react";

import { cn } from "@/lib/utils";

function NavItem({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={to}
      end
      className={({ isActive }) =>
        cn(
          "flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
          isActive
            ? "bg-primary/10 text-primary"
            : "text-muted-foreground hover:text-foreground hover:bg-accent",
        )
      }
    >
      {children}
    </NavLink>
  );
}

export default function Layout() {
  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-40 border-b bg-background/80 backdrop-blur-sm">
        <div className="mx-auto flex h-14 max-w-[1440px] items-center gap-6 px-6">
          <Link to="/" className="flex items-center gap-2 font-semibold tracking-tight">
            <TrendingUp className="size-5 text-primary" />
            <span>Skillsight</span>
          </Link>

          <nav className="flex items-center gap-1">
            <NavItem to="/">
              <Home className="size-4" />
              Leaderboard
            </NavItem>
            <NavItem to="/stats">
              <BarChart3 className="size-4" />
              Analytics
            </NavItem>
          </nav>

          <div className="ml-auto flex items-center gap-2">
            <span className="rounded-full bg-primary/10 px-2.5 py-0.5 font-mono text-xs text-primary">
              v0
            </span>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1440px] px-6 py-6">
        <Outlet />
      </main>
    </div>
  );
}
