import React from "react";
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
    <div className="flex flex-row gap-2 py-2.5 px-5 border-b border-white/10">
      <Button
        size="sm"
        variant="secondary"
        className="btn-secondary text-[11px] px-2.5 py-1 h-7 flex-1 border border-transparent hover:border-white/40 hover:brightness-110 hover:scale-[1.02] hover:shadow-none transition-all"
        disabled={actionMutation.isPending}
        onClick={(e) => {
          e.stopPropagation();
          actionMutation.mutate({ id: inst.instance_id, action: "start" });
        }}
      >
        <Play className="mr-1 h-3 w-3" /> Start
      </Button>
      <Button
        size="sm"
        variant="outline"
        className="text-[11px] px-2.5 py-1 h-7 flex-1 border-white/20 bg-white/5 hover:bg-white/15 hover:border-white/40 hover:text-foreground hover:scale-[1.02] hover:shadow-none transition-all"
        disabled={actionMutation.isPending}
        onClick={(e) => {
          e.stopPropagation();
          actionMutation.mutate({ id: inst.instance_id, action: "stop" });
        }}
      >
        <Power className="mr-1 h-3 w-3" /> Stop
      </Button>
      <Button
        size="sm"
        variant="destructive"
        className="btn-danger text-[11px] px-2.5 py-1 h-7 flex-1 border border-transparent hover:border-white/40 hover:brightness-110 hover:scale-[1.02] hover:shadow-none transition-all"
        disabled={actionMutation.isPending}
        onClick={(e) => {
          e.stopPropagation();
          actionMutation.mutate({ id: inst.instance_id, action: "terminate" });
        }}
      >
        <Trash2 className="mr-1 h-3 w-3" /> Terminate
      </Button>
    </div>
  );

  return (
    <div className="space-y-4 w-full">
      <div className="flex items-center justify-between text-sm text-blue-200">
        <span>{isFetching ? "Syncing latest state…" : "Auto-refresh every 5s"}</span>
        <Button variant="outline" size="sm" onClick={onRefresh} className="border-white/20 bg-white/5 hover:bg-white/15 hover:border-white/40 hover:text-foreground hover:scale-[1.02] hover:shadow-none transition-all">
          Manual refresh
        </Button>
      </div>

      {error ? (
        <div className="text-sm text-red-300">Error loading instances: {error.message}</div>
      ) : null}

      <div className="rounded-xl border border-white/10 bg-black/20 w-full overflow-hidden shadow-lg">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[12%] text-center">ID</TableHead>
              <TableHead className="w-[16%] text-center">Name</TableHead>
              <TableHead className="w-[9%] text-center">Type</TableHead>
              <TableHead className="w-[14%] text-center">State</TableHead>
              <TableHead className="w-[12%] text-center">AZ</TableHead>
              <TableHead className="w-[12%] pl-4 text-center">Private IP</TableHead>
              <TableHead className="w-[12%] pl-4 text-center">Public IP</TableHead>
              <TableHead className="w-[13%] text-center">Volumes</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={8} className="text-center">
                  Loading instances...
                </TableCell>
              </TableRow>
            ) : data && data.length > 0 ? (
              data.map((inst, idx) => {
                const instanceId = inst.instance_id ?? `row-${idx}`;
                const isSelected = selectedId === inst.instance_id;
                return (
                  <React.Fragment key={instanceId}>
                    <TableRow
                      className={`cursor-pointer transition-all duration-200 ${
                        isSelected 
                          ? "bg-primary/8 hover:bg-primary/10" 
                          : "bg-black/10 hover:bg-black/20"
                      }`}
                      onClick={() => onSelect?.(inst)}
                    >
                      <TableCell className="font-mono whitespace-nowrap font-medium text-center" title={inst.instance_id ?? "-"}>
                        <span className={isSelected ? "text-primary" : "text-foreground"}>
                          {inst.instance_id ?? "-"}
                        </span>
                      </TableCell>
                      <TableCell className="whitespace-nowrap font-medium text-center" title={inst.name_tag ?? "-"}>
                        {inst.name_tag ?? "-"}
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-muted-foreground text-center">{inst.instance_type}</TableCell>
                      <TableCell className="whitespace-nowrap text-center pr-4">
                        <Badge variant={stateVariant[inst.state]} className="text-[10px] px-2.5 py-0.5 whitespace-nowrap font-medium">
                          {inst.state}
                        </Badge>
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-muted-foreground text-center pl-4">{inst.availability_zone ?? "-"}</TableCell>
                      <TableCell className="font-mono whitespace-nowrap pl-4 text-muted-foreground text-center" title={inst.private_ip ?? "-"}>
                        {inst.private_ip ?? "-"}
                      </TableCell>
                      <TableCell className="font-mono whitespace-nowrap pl-4 text-muted-foreground text-center" title={inst.public_ip ?? "-"}>
                        {inst.public_ip ?? "-"}
                      </TableCell>
                      <TableCell className="text-center whitespace-nowrap font-medium">{volumesByInstance?.[inst.instance_id] ?? 0}</TableCell>
                    </TableRow>
                    {inst.instance_id && (
                      <TableRow className={`${isSelected ? "bg-primary/5" : "bg-black/30"} hover:bg-black/35 transition-colors`}>
                        <TableCell colSpan={8} className="p-0">
                          {renderActions(inst)}
                        </TableCell>
                      </TableRow>
                    )}
                  </React.Fragment>
                );
              })
            ) : (
              <TableRow>
                <TableCell colSpan={8} className="text-center">
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


