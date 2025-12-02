import { api } from "../../lib/api";
import { InstanceDto } from "./types";

type RawInstance = {
  AccountId?: string;
  InstanceId?: string;
  InstanceType?: string;
  State?: string;
  AvailabilityZone?: string;
  PrivateIpAddress?: string;
  PublicIpAddress?: string;
  KeyName?: string;
  NameTag?: string;
};

const toInstanceDto = (raw: RawInstance, idx: number): InstanceDto => ({
  account_id: raw.AccountId ?? "",
  instance_id: raw.InstanceId ?? `mock-${idx}`,
  instance_type: raw.InstanceType ?? "-",
  state: (raw.State as InstanceDto["state"]) ?? "stopped",
  availability_zone: raw.AvailabilityZone ?? "-",
  private_ip: raw.PrivateIpAddress ?? "-",
  public_ip: raw.PublicIpAddress ?? "-",
  key_name: raw.KeyName ?? "-",
  name_tag: raw.NameTag ?? "-",
});

export async function fetchInstances(): Promise<InstanceDto[]> {
  const res = await api.get<RawInstance[]>("/instances");
  return res.data.map(toInstanceDto);
}

export async function throttledRefresh(): Promise<InstanceDto[]> {
  const res = await api.get<InstanceDto[]>("/instances/throttled");
  return res.data;
}

export async function mutateInstance(instanceId: string, action: "start" | "stop" | "terminate") {
  await api.post(`/instances/${instanceId}/${action}`);
}


