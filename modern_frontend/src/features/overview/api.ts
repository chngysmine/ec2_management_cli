import { api } from "../../lib/api";
import { InstanceDto } from "../instances/types";
import { VolumeDto } from "../volumes/types";
import { EventEntryDto } from "../events/api";
import { EconomicsPoint } from "../dashboard/types";

export interface OverviewStats {
  total_instances: number;
  running_instances: number;
  stopped_instances: number;
  total_volumes: number;
  available_volumes: number;
  quota_used_gib: number;
  quota_total_gib: number;
}

export interface OverviewPayload {
  mode: string;
  region: string;
  generated_at: string;
  stats: OverviewStats;
  instances: InstanceDto[];
  volumes: VolumeDto[];
  events: EventEntryDto[];
  metrics: EconomicsPoint[];
}

export async function fetchOverview(): Promise<OverviewPayload> {
  const res = await api.get<OverviewPayload>("/overview");
  return res.data;
}


