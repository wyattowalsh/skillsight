import { BrowserRouter, Route, Routes } from "react-router-dom";

import Layout from "@/components/Layout";
import LeaderboardPage from "@/pages/LeaderboardPage";
import SkillDetailPage from "@/pages/SkillDetailPage";
import StatsPage from "@/pages/StatsPage";
import { TooltipProvider } from "@/components/ui/tooltip";

export default function App() {
  return (
    <BrowserRouter>
      <TooltipProvider>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<LeaderboardPage />} />
            <Route path="/skill/:id" element={<SkillDetailPage />} />
            <Route path="/stats" element={<StatsPage />} />
            <Route
              path="*"
              element={
                <div className="flex flex-col items-center justify-center min-h-[50vh] gap-4">
                  <h1 className="text-2xl font-bold">Page not found</h1>
                  <p className="text-muted-foreground">
                    The page you&apos;re looking for doesn&apos;t exist.
                  </p>
                  <a href="/" className="text-primary hover:underline">
                    Go to leaderboard
                  </a>
                </div>
              }
            />
          </Route>
        </Routes>
      </TooltipProvider>
    </BrowserRouter>
  );
}
