import * as React from "react";
import { Command as Cmdk } from "cmdk";
import { Search } from "lucide-react";
import { cn } from "../../lib/utils";

const Command = React.forwardRef<
  React.ElementRef<typeof Cmdk>,
  React.ComponentPropsWithoutRef<typeof Cmdk>
>(({ className, ...props }, ref) => (
  <Cmdk
    ref={ref}
    className={cn(
      "flex h-full w-full flex-col overflow-hidden rounded-md bg-popover text-popover-foreground",
      className
    )}
    {...props}
  />
));
Command.displayName = "Command";

const CommandInput = React.forwardRef<
  React.ElementRef<typeof Cmdk.Input>,
  React.ComponentPropsWithoutRef<typeof Cmdk.Input>
>(({ className, ...props }, ref) => (
  <div className="flex items-center border-b px-3" cmdk-input-wrapper="">
    <Search className="mr-2 h-4 w-4 shrink-0 opacity-50" />
    <Cmdk.Input
      ref={ref}
      className={cn(
        "flex h-11 w-full rounded-md bg-transparent py-3 text-sm outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50",
        className
      )}
      {...props}
    />
  </div>
));

CommandInput.displayName = "CommandInput";

const CommandList = React.forwardRef<
  React.ElementRef<typeof Cmdk.List>,
  React.ComponentPropsWithoutRef<typeof Cmdk.List>
>(({ className, ...props }, ref) => (
  <Cmdk.List
    ref={ref}
    className={cn("max-h-[300px] overflow-y-auto overflow-x-hidden", className)}
    {...props}
  />
));

CommandList.displayName = "CommandList";

const CommandEmpty = React.forwardRef<
  React.ElementRef<typeof Cmdk.Empty>,
  React.ComponentPropsWithoutRef<typeof Cmdk.Empty>
>((props, ref) => (
  <Cmdk.Empty ref={ref} className="py-6 text-center text-sm" {...props} />
));

CommandEmpty.displayName = "CommandEmpty";

const CommandGroup = React.forwardRef<
  React.ElementRef<typeof Cmdk.Group>,
  React.ComponentPropsWithoutRef<typeof Cmdk.Group>
>(({ className, ...props }, ref) => (
  <Cmdk.Group
    ref={ref}
    className={cn(
      "overflow-hidden p-1 text-foreground [&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-medium [&_[cmdk-group-heading]]:text-muted-foreground",
      className
    )}
    {...props}
  />
));

CommandGroup.displayName = "CommandGroup";

const CommandSeparator = React.forwardRef<
  React.ElementRef<typeof Cmdk.Separator>,
  React.ComponentPropsWithoutRef<typeof Cmdk.Separator>
>(({ className, ...props }, ref) => (
  <Cmdk.Separator
    ref={ref}
    className={cn("-mx-1 h-px bg-border", className)}
    {...props}
  />
));
CommandSeparator.displayName = "CommandSeparator";

const CommandItem = React.forwardRef<
  React.ElementRef<typeof Cmdk.Item>,
  React.ComponentPropsWithoutRef<typeof Cmdk.Item>
>(({ className, ...props }, ref) => (
  <Cmdk.Item
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center rounded-sm px-2 py-1.5 text-sm outline-none aria-selected:bg-accent aria-selected:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      className
    )}
    {...props}
  />
));

CommandItem.displayName = "CommandItem";

const CommandShortcut = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLSpanElement>) => {
  return (
    <span
      className={cn(
        "ml-auto text-xs tracking-widest text-muted-foreground",
        className
      )}
      {...props}
    />
  );
};
CommandShortcut.displayName = "CommandShortcut";

export {
  Command,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandShortcut,
  CommandSeparator,
};

