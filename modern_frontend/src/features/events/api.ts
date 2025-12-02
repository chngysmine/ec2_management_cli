import { api } from "../../lib/api";

export interface EventEntryDto {
  id: string;
  timestamp: string;
  level: "info" | "warning" | "error";
  message: string;
  context?: Record<string, unknown>;
}

export async function fetchEvents(limit = 50): Promise<EventEntryDto[]> {
  const res = await api.get<EventEntryDto[]>(`/events?limit=${limit}`);
  return res.data;
}


