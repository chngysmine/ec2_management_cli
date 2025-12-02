export interface VolumeDto {
  volume_id: string;
  size_gib: number;
  state: string;
  volume_type?: string | null;
  availability_zone?: string | null;
  throughput?: number | null;
  iops?: number | null;
  attached_instances: string[];
}


