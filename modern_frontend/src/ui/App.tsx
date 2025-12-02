import { useState, useMemo } from "react";
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

export const App = () => {
  const [selectedInstance, setSelectedInstance] = useState<InstanceDto | null>(null);
  const queryClient = useQueryClient();

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
    <div className="bg-terminal text-gray-100 min-h-screen">
      <div className="relative z-10 h-full flex flex-col">
        <header className="glass border-b border-white/10">
          <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
            <div>
              <p className="text-xs uppercase tracking-wide text-blue-200 mb-1">Cloud-native control plane</p>
              <h1 className="text-2xl font-bold text-white">Modern EC2 Manager</h1>
            </div>
            <div className="text-sm text-blue-200">{overview ? `Region: ${overview.region}` : "Loading…"}</div>
          </div>
        </header>

        <main className="flex-1 overflow-auto px-6 py-6">
          <div className="max-w-6xl mx-auto space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
              <Card className="stat-card rounded-lg p-4">
                <p className="text-xs text-blue-200 uppercase">Instances</p>
                <p className="text-3xl font-bold text-blue-400">{stats?.total_instances ?? "–"}</p>
                <p className="text-xs text-blue-200">
                  {stats?.running_instances ?? 0} running · {stats?.stopped_instances ?? 0} stopped
                </p>
              </Card>
              <Card className="stat-card rounded-lg p-4">
                <p className="text-xs text-green-200 uppercase">Volumes</p>
                <p className="text-3xl font-bold text-green-400">{stats?.total_volumes ?? "–"}</p>
                <p className="text-xs text-green-200">{stats?.quota_used_gib ?? 0} GiB used</p>
              </Card>
              <Card className="stat-card rounded-lg p-4">
                <p className="text-xs text-purple-200 uppercase">Quota</p>
                <p className="text-3xl font-bold text-purple-400">{quotaPct}%</p>
                <p className="text-xs text-purple-200">of {stats?.quota_total_gib ?? 1024} GiB</p>
              </Card>
              <div className="glass rounded-lg p-4">
                <label className="block text-xs text-gray-300 mb-2">Selected instance</label>
                <div className="text-sm text-white h-10 flex items-center">{selectedInstance?.instance_id ?? "None"}</div>
              </div>
              <Card className="stat-card rounded-lg p-4">
                <p className="text-xs text-blue-200 uppercase">Status</p>
                <p className="text-3xl font-bold text-blue-400">Ready</p>
                <p className="text-xs text-blue-200">Auto-refresh every 4s</p>
              </Card>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <div className="glass rounded-xl p-4">
                <p className="text-white font-semibold mb-3">Terminal / Activity</p>
                <div className="h-64 lg:h-80 overflow-hidden">
                  <EventLog events={overview?.events} />
                </div>
              </div>

              <div className="glass rounded-xl p-4 flex flex-col space-y-4">
                <div className="flex items-center justify-between">
                  <p className="text-white font-semibold">Instances</p>
                  <div className="space-x-2">
                    <Button size="sm" variant="outline" onClick={() => overviewQuery.refetch()}>
                      Manual refresh
                    </Button>
                  </div>
                </div>
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
            </div>

            <div className="grid lg:grid-cols-5 gap-4">
              <div className="lg:col-span-3 glass rounded-xl p-4">
                <UnitEconomicsChart data={overview?.metrics ?? []} />
              </div>
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
    </div>
  );
};
