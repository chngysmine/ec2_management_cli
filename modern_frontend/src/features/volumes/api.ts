import { api } from "../../lib/api";
import { VolumeDto } from "./types";

type RawVolume = {
  VolumeId?: string;
  SizeGiB?: number;
  State?: string;
  AvailabilityZone?: string;
  AttachedInstances?: string[];
};

const toVolumeDto = (raw: RawVolume, idx: number): VolumeDto => ({
  volume_id: raw.VolumeId ?? `vol-mock-${idx}`,
  size_gib: raw.SizeGiB ?? 0,
  state: raw.State ?? "-",
  availability_zone: raw.AvailabilityZone ?? "-",
  attached_instances: raw.AttachedInstances ?? [],
});

export async function fetchVolumes(): Promise<VolumeDto[]> {
  const res = await api.get<RawVolume[]>("/volumes");
  return res.data.map(toVolumeDto);
}

export async function createVolume(input: { size_gib: number; availability_zone?: string }) {
  const res = await api.post("/volumes", input);
  return res.data;
}

export async function attachVolume(input: { volume_id: string; instance_id: string; device_name: string }) {
  const res = await api.post("/volumes/attach", input);
  return res.data;
}

export async function detachVolume(input: { volume_id: string; force?: boolean }) {
  const res = await api.post("/volumes/detach", input);
  return res.data;
}


