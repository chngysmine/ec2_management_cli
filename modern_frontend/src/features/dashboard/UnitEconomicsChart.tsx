import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  ComposedChart,
  Legend,
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../components/ui/card";
import { EconomicsPoint } from "./types";

type Props = {
  data: EconomicsPoint[];
};

export function UnitEconomicsChart({ data }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Unit Economics</CardTitle>
        <CardDescription>Cost per hour vs CPU utilization (mock realtime)</CardDescription>
      </CardHeader>
      <CardContent className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data ?? []}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
            <XAxis dataKey="timestamp" tickFormatter={(value) => new Date(value).toLocaleTimeString()} />
            <YAxis
              yAxisId="left"
              orientation="left"
              stroke="#2563eb"
              label={{ value: "Cost $/hr", angle: -90, position: "insideLeft" }}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              stroke="#16a34a"
              label={{ value: "CPU %", angle: 90, position: "insideRight" }}
            />
            <Tooltip />
            <Legend />
            <Area
                yAxisId="left"
                type="monotone"
                dataKey="cost_per_hour"
                stroke="#2563eb"
                fill="#2563eb22"
                name="Cost ($/hr)"
              />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="cpu_utilization"
              stroke="#16a34a"
              dot={false}
              strokeWidth={2}
              name="CPU %"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}


