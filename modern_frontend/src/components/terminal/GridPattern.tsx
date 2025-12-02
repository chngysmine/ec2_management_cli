import { cn } from "../../lib/utils";

interface GridPatternProps {
  className?: string;
  size?: number;
  strokeWidth?: number;
}

export function GridPattern({ className, size = 20, strokeWidth = 0.5 }: GridPatternProps) {
  return (
    <div
      className={cn("absolute inset-0 grid-pattern", className)}
      style={{
        backgroundSize: `${size}px ${size}px`,
      }}
    />
  );
}

