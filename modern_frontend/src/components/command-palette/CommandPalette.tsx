import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "../ui/command";
import {
  Dialog,
  DialogContent,
} from "../ui/dialog";
import { fetchOverview } from "../../features/overview/api";
import { InstanceDto } from "../../features/instances/types";
import { VolumeDto } from "../../features/volumes/types";

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSelectInstance?: (instance: InstanceDto) => void;
  onSelectVolume?: (volume: VolumeDto) => void;
}

export function CommandPalette({
  open,
  onOpenChange,
  onSelectInstance,
  onSelectVolume,
}: CommandPaletteProps) {
  const { data: overview } = useQuery({
    queryKey: ["overview"],
    queryFn: fetchOverview,
  });

  const instances = overview?.instances ?? [];
  const volumes = overview?.volumes ?? [];

  const handleSelect = (value: string) => {
    if (value.startsWith("instance:")) {
      const instanceId = value.replace("instance:", "");
      const instance = instances.find((i) => i.instance_id === instanceId);
      if (instance && onSelectInstance) {
        onSelectInstance(instance);
        onOpenChange(false);
      }
    } else if (value.startsWith("volume:")) {
      const volumeId = value.replace("volume:", "");
      const volume = volumes.find((v) => v.volume_id === volumeId);
      if (volume && onSelectVolume) {
        onSelectVolume(volume);
        onOpenChange(false);
      }
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="overflow-hidden p-0">
        <Command className="rounded-lg border shadow-md">
          <CommandInput placeholder="Type a command or search..." />
          <CommandList>
            <CommandEmpty>No results found.</CommandEmpty>
            
            {instances.length > 0 && (
              <CommandGroup heading="Instances">
                {instances.map((instance) => (
                  <CommandItem
                    key={instance.instance_id}
                    value={`instance:${instance.instance_id}`}
                    onSelect={handleSelect}
                  >
                    <span className="font-mono text-xs">{instance.instance_id}</span>
                    {instance.name_tag && (
                      <span className="ml-2 text-muted-foreground">{instance.name_tag}</span>
                    )}
                    <CommandShortcut>{instance.state}</CommandShortcut>
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            {volumes.length > 0 && (
              <CommandGroup heading="Volumes">
                {volumes.map((volume) => (
                  <CommandItem
                    key={volume.volume_id}
                    value={`volume:${volume.volume_id}`}
                    onSelect={handleSelect}
                  >
                    <span className="font-mono text-xs">{volume.volume_id}</span>
                    <span className="ml-2 text-muted-foreground">{volume.size_gib} GiB</span>
                    <CommandShortcut>{volume.state}</CommandShortcut>
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            <CommandGroup heading="Actions">
              <CommandItem value="refresh" onSelect={() => window.location.reload()}>
                Refresh Data
                <CommandShortcut>⌘R</CommandShortcut>
              </CommandItem>
              <CommandItem value="ai-chat" onSelect={() => {/* TODO: Open AI chat */}}>
                Ask AI Assistant
                <CommandShortcut>⌘K</CommandShortcut>
              </CommandItem>
            </CommandGroup>
          </CommandList>
        </Command>
      </DialogContent>
    </Dialog>
  );
}

