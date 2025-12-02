import { api } from "../../lib/api";
import { EconomicsPoint } from "./types";

export async function fetchEconomics(): Promise<EconomicsPoint[]> {
  const res = await api.get<EconomicsPoint[]>("/economics");
  return res.data;
}


