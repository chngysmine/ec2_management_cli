import { ReactNode } from "react";
import { GridPattern } from "./GridPattern";
import { cn } from "../../lib/utils";

interface ShellLayoutProps {
  children: ReactNode;
  className?: string;
  showScanlines?: boolean;
}

export function ShellLayout({ children, className, showScanlines = true }: ShellLayoutProps) {
  return (
    <div className={cn("relative min-h-screen bg-background", className)}>
      {/* Grid Pattern Background */}
      <GridPattern className="opacity-40" />
      
      {/* CRT Scanlines Overlay */}
      {showScanlines && (
        <div className="crt-scanlines fixed inset-0 pointer-events-none z-50" />
      )}
      
      {/* Content */}
      <div className="relative z-10">
        {children}
      </div>
    </div>
  );
}

