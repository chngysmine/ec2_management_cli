import { useState, useMemo, useEffect } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { InstanceTable } from "../features/instances/InstanceTable";
import { VolumePanel } from "../features/volumes/VolumePanel";
import { UnitEconomicsChart } from "../features/dashboard/UnitEconomicsChart";
import { EventLog } from "../features/events/EventLog";
import { InstanceDto } from "../features/instances/types";
import { Button } from "../components/ui/button";
import { Card } from "../components/ui/card";
import { fetchOverview } from "../features/overview/api";
import { VolumeDto } from "../features/volumes/types";
import { ShellLayout } from "../components/terminal/ShellLayout";
import { CommandPalette } from "../components/command-palette/CommandPalette";

export const App = () => {
  const [selectedInstance, setSelectedInstance] = useState<InstanceDto | null>(null);
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);
  const queryClient = useQueryClient();

  // Command Palette keyboard shortcut (Cmd+K / Ctrl+K)
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setCommandPaletteOpen((open) => !open);
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const overviewQuery = useQuery({
    queryKey: ["overview"],
    queryFn: fetchOverview,
    refetchInterval: 4000,
  });

  const overview = overviewQuery.data;
  const stats = overview?.stats;
  const quotaPct =
    stats && stats.quota_total_gib > 0 ? Math.round((stats.quota_used_gib / stats.quota_total_gib) * 100) : 0;

  const volumesByInstance = useMemo(() => {
    const map: Record<string, number> = {};
    (overview?.volumes ?? []).forEach((v) => {
      (v.attached_instances ?? []).forEach((id) => {
        map[id] = (map[id] ?? 0) + 1;
      });
    });
    return map;
  }, [overview?.volumes]);

  return (
    <ShellLayout showScanlines={true}>
      <div className="relative z-10 h-full flex flex-col">
        <header className="border-b border-border bg-card/50 backdrop-blur-sm">
          <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
            <div>
              <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1 font-mono">
                Cloud-native control plane
              </p>
              <h1 className="text-2xl font-bold text-foreground font-mono">Modern EC2 Manager</h1>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-sm text-muted-foreground font-mono">
                {overview ? `Region: ${overview.region}` : "Loading…"}
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setCommandPaletteOpen(true)}
                className="font-mono text-xs border-white/20 bg-white/5 hover:bg-white/15 hover:border-white/40 hover:text-foreground hover:scale-[1.02] hover:shadow-none transition-all"
              >
                <span className="hidden sm:inline">Command Palette</span>
                <span className="sm:hidden">⌘K</span>
              </Button>
            </div>
          </div>
        </header>

        <main className="flex-1 overflow-auto px-6 py-6">
          <div className="max-w-7xl mx-auto space-y-6">
            {/* Bento Grid - Stats Dashboard */}
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
              <Card className="bg-card border-border p-4">
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-mono mb-2">Instances</p>
                <p className="text-3xl font-bold text-foreground font-mono">{stats?.total_instances ?? "–"}</p>
                <p className="text-xs text-muted-foreground font-mono mt-1">
                  {stats?.running_instances ?? 0} running · {stats?.stopped_instances ?? 0} stopped
                </p>
              </Card>
              <Card className="bg-card border-border p-4">
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-mono mb-2">Volumes</p>
                <p className="text-3xl font-bold text-terminal-green font-mono">{stats?.total_volumes ?? "–"}</p>
                <p className="text-xs text-muted-foreground font-mono mt-1">{stats?.quota_used_gib ?? 0} GiB used</p>
              </Card>
              <Card className="bg-card border-border p-4">
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-mono mb-2">Quota</p>
                <p className="text-3xl font-bold text-foreground font-mono">{quotaPct}%</p>
                <p className="text-xs text-muted-foreground font-mono mt-1">of {stats?.quota_total_gib ?? 1024} GiB</p>
              </Card>
              <Card className="bg-card border-border p-4">
                <label className="block text-xs text-muted-foreground mb-2 font-mono uppercase tracking-wider">Selected Instance</label>
                <div className="text-sm text-foreground h-10 flex items-center font-mono">
                  {selectedInstance?.instance_id ?? "None"}
                </div>
              </Card>
              <Card className="bg-card border-border p-4">
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-mono mb-2">Status</p>
                <p className="text-3xl font-bold text-terminal-green font-mono">Ready</p>
                <p className="text-xs text-muted-foreground font-mono mt-1">Auto-refresh every 4s</p>
              </Card>
            </div>

            {/* Bento Grid - Main Content */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Card className="bg-card border-border p-4">
                <p className="text-foreground font-semibold mb-3 font-mono">Terminal / Activity</p>
                <div className="h-64 lg:h-80 overflow-hidden">
                  <EventLog events={overview?.events} />
                </div>
              </Card>

              <Card className="bg-card border-border p-4 flex flex-col space-y-4 min-w-0 overflow-hidden">
                <div className="flex items-center justify-between">
                  <p className="text-foreground font-semibold font-mono">Instances</p>
                  <div className="space-x-2">
                    <Button size="sm" variant="outline" onClick={() => overviewQuery.refetch()} className="font-mono text-xs border-white/20 bg-white/5 hover:bg-white/15 hover:border-white/40 hover:text-foreground hover:scale-[1.02] hover:shadow-none transition-all">
                      Refresh
                    </Button>
                  </div>
                </div>
                <div className="min-w-0 w-full">
                  <InstanceTable
                    data={overview?.instances}
                    isLoading={overviewQuery.isLoading}
                    isFetching={overviewQuery.isFetching}
                    error={overviewQuery.error as Error | null}
                    onRefresh={() => overviewQuery.refetch()}
                    onSelect={(inst) => setSelectedInstance(inst)}
                    selectedId={selectedInstance?.instance_id ?? null}
                    volumesByInstance={volumesByInstance}
                  />
                </div>
              </Card>
            </div>

            {/* Bento Grid - Charts & Volumes */}
            <div className="grid lg:grid-cols-5 gap-4">
              <Card className="lg:col-span-3 bg-card border-border p-4">
                <UnitEconomicsChart data={overview?.metrics ?? []} />
              </Card>
              <div className="lg:col-span-2">
                <VolumePanel
                  volumes={overview?.volumes as VolumeDto[]}
                  isLoading={overviewQuery.isLoading}
                  error={overviewQuery.error as Error | null}
                  selectedInstance={selectedInstance}
                />
              </div>
            </div>
          </div>
        </main>
      </div>

      {/* Command Palette */}
      <CommandPalette
        open={commandPaletteOpen}
        onOpenChange={setCommandPaletteOpen}
        onSelectInstance={setSelectedInstance}
      />
    </ShellLayout>
  );
};
