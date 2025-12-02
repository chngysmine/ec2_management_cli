import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Play, Power, Trash2 } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Badge } from "../../components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "../../components/ui/table";
import { mutateInstance } from "./api";
import { InstanceDto, InstanceState } from "./types";

const stateVariant: Record<InstanceState, "default" | "secondary" | "destructive" | "outline"> = {
  running: "default",
  pending: "secondary",
  stopping: "secondary",
  stopped: "outline",
  "shutting-down": "destructive",
  terminated: "destructive",
  impaired: "destructive",
};

type Props = {
  data: InstanceDto[] | undefined;
  isLoading: boolean;
  isFetching: boolean;
  error: Error | null;
  onRefresh: () => void;
  onSelect?: (inst: InstanceDto) => void;
  selectedId?: string | null;
  volumesByInstance?: Record<string, number>;
};

export function InstanceTable({
  data,
  isLoading,
  isFetching,
  error,
  onRefresh,
  onSelect,
  selectedId,
  volumesByInstance,
}: Props) {
  const queryClient = useQueryClient();

  const actionMutation = useMutation({
    mutationFn: ({ id, action }: { id: string; action: "start" | "stop" | "terminate" }) =>
      mutateInstance(id, action),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["overview"] });
    },
  });

  const renderActions = (inst: InstanceDto) => (
    <div className="flex gap-2">
      <Button
        size="sm"
        variant="secondary"
        className="btn-secondary"
        disabled={actionMutation.isPending}
        onClick={() => actionMutation.mutate({ id: inst.instance_id, action: "start" })}
      >
        <Play className="mr-1 h-3.5 w-3.5" /> Start
      </Button>
      <Button
        size="sm"
        variant="outline"
        disabled={actionMutation.isPending}
        onClick={() => actionMutation.mutate({ id: inst.instance_id, action: "stop" })}
      >
        <Power className="mr-1 h-3.5 w-3.5" /> Stop
      </Button>
      <Button
        size="sm"
        variant="destructive"
        className="btn-danger"
        disabled={actionMutation.isPending}
        onClick={() => actionMutation.mutate({ id: inst.instance_id, action: "terminate" })}
      >
        <Trash2 className="mr-1 h-3.5 w-3.5" /> Terminate
      </Button>
    </div>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between text-sm text-blue-200">
        <span>{isFetching ? "Syncing latest state…" : "Auto-refresh every 5s"}</span>
        <Button variant="outline" size="sm" onClick={onRefresh}>
          Manual refresh
        </Button>
      </div>

      {error ? (
        <div className="text-sm text-red-300">Error loading instances: {error.message}</div>
      ) : null}

      <div className="rounded-xl border border-white/10 bg-black/20">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ID</TableHead>
              <TableHead>Name</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>State</TableHead>
              <TableHead>AZ</TableHead>
              <TableHead>Private IP</TableHead>
              <TableHead>Public IP</TableHead>
              <TableHead>Volumes</TableHead>
              <TableHead>Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={9} className="text-center">
                  Loading instances...
                </TableCell>
              </TableRow>
            ) : data && data.length > 0 ? (
              data.map((inst, idx) => (
                <TableRow
                  key={inst.instance_id ?? inst.name_tag ?? idx}
                  className={selectedId === inst.instance_id ? "bg-primary/5" : undefined}
                  onClick={() => onSelect?.(inst)}
                >
                  <TableCell className="font-mono text-xs">{inst.instance_id ?? "-"}</TableCell>
                  <TableCell>{inst.name_tag ?? "-"}</TableCell>
                  <TableCell>{inst.instance_type}</TableCell>
                  <TableCell>
                    <Badge variant={stateVariant[inst.state]}>{inst.state}</Badge>
                  </TableCell>
                  <TableCell>{inst.availability_zone ?? "-"}</TableCell>
                  <TableCell>{inst.private_ip ?? "-"}</TableCell>
                  <TableCell>{inst.public_ip ?? "-"}</TableCell>
                  <TableCell>{volumesByInstance?.[inst.instance_id] ?? 0}</TableCell>
                  <TableCell>{inst.instance_id ? renderActions(inst) : "N/A"}</TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={9} className="text-center">
                  No instances
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}


