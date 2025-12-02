import { useMutation, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Link2, Plus, Unplug } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "../../components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "../../components/ui/table";
import { Badge } from "../../components/ui/badge";
import { attachVolume, createVolume, detachVolume } from "./api";
import { VolumeDto } from "./types";
import { InstanceDto } from "../instances/types";

type Props = {
  volumes: VolumeDto[] | undefined;
  isLoading: boolean;
  error: Error | null;
  selectedInstance?: InstanceDto | null;
};

export function VolumePanel({ volumes, isLoading, error, selectedInstance }: Props) {
  const qc = useQueryClient();
  const createMutation = useMutation({
    mutationFn: () => createVolume({ size_gib: 20, availability_zone: selectedInstance?.availability_zone ?? "us-east-1a" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["overview"] }),
  });

  const attachMutation = useMutation({
    mutationFn: ({ volumeId, instanceId }: { volumeId: string; instanceId: string }) =>
      attachVolume({ volume_id: volumeId, instance_id: instanceId, device_name: "/dev/sdf" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["overview"] }),
  });

  const detachMutation = useMutation({
    mutationFn: (volumeId: string) => detachVolume({ volume_id: volumeId }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["overview"] }),
  });

  const canAttach = (vol: VolumeDto) => selectedInstance && vol.state === "available";
  const canDetach = (vol: VolumeDto) => vol.state === "in-use";

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <div>
          <CardTitle className="text-lg">Volumes</CardTitle>
          <p className="text-sm text-muted-foreground">
            Quota-aware mock volumes {selectedInstance ? `(selected: ${selectedInstance.instance_id})` : ""}
          </p>
        </div>
        <Button onClick={() => createMutation.mutate()} disabled={createMutation.isPending} className="btn-secondary">
          <Plus className="mr-2 h-4 w-4" /> New volume
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        {error ? (
          <div className="flex items-center gap-2 rounded-lg border border-destructive/40 bg-destructive/10 p-2 text-sm text-destructive">
            <AlertTriangle className="h-4 w-4" />
            {error.message}
          </div>
        ) : null}
        {createMutation.isError && (
          <div className="flex items-center gap-2 rounded-lg border border-orange-200 bg-orange-50 p-2 text-sm text-orange-700">
            <AlertTriangle className="h-4 w-4" />
            {(createMutation.error as Error).message}
          </div>
        )}
        {(attachMutation.isError || detachMutation.isError) && (
          <div className="flex items-center gap-2 rounded-lg border border-orange-200 bg-orange-50 p-2 text-sm text-orange-700">
            <AlertTriangle className="h-4 w-4" />
            {((attachMutation.error || detachMutation.error) as Error).message}
          </div>
        )}

        <div className="rounded-xl border border-white/10 bg-black/20">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Size</TableHead>
                <TableHead>State</TableHead>
                <TableHead>AZ</TableHead>
                <TableHead>Attached</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center">
                    Loading...
                  </TableCell>
                </TableRow>
              ) : volumes && volumes.length > 0 ? (
                volumes.map((vol) => (
                  <TableRow key={vol.volume_id ?? `vol-${vol.size_gib}-${vol.state}`}>
                    <TableCell className="font-mono text-xs">{vol.volume_id ?? "-"}</TableCell>
                    <TableCell>{vol.size_gib} GiB</TableCell>
                    <TableCell>
                      <Badge variant={vol.state === "available" ? "outline" : "secondary"}>{vol.state}</Badge>
                    </TableCell>
                    <TableCell>{vol.availability_zone ?? "-"}</TableCell>
                    <TableCell>
                      {(vol.attached_instances ?? []).length > 0 ? vol.attached_instances!.join(", ") : "None"}
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-2">
                        <Button
                          size="sm"
                          variant="secondary"
                          className="btn-secondary"
                          disabled={!canAttach(vol) || attachMutation.isPending}
                          onClick={() => {
                            if (!selectedInstance) {
                              alert("Select an instance to attach this volume.");
                              return;
                            }
                            attachMutation.mutate({ volumeId: vol.volume_id, instanceId: selectedInstance.instance_id });
                          }}
                        >
                          <Link2 className="mr-1 h-3.5 w-3.5" /> Attach
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="btn-danger"
                          disabled={!canDetach(vol) || detachMutation.isPending}
                          onClick={() => detachMutation.mutate(vol.volume_id)}
                        >
                          <Unplug className="mr-1 h-3.5 w-3.5" /> Detach
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={6} className="text-center">
                    No volumes
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

