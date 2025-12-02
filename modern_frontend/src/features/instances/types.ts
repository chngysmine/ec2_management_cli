export type InstanceState =
  | "pending"
  | "running"
  | "stopping"
  | "stopped"
  | "shutting-down"
  | "terminated"
  | "impaired";

export interface InstanceDto {
  account_id: string;
  instance_id: string;
  instance_type: string;
  state: InstanceState;
  availability_zone?: string | null;
  private_ip?: string | null;
  public_ip?: string | null;
  key_name?: string | null;
  name_tag?: string | null;
}


