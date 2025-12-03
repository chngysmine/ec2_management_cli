import { useState, useRef, useEffect } from "react";
import { Button } from "../ui/button";
import { cn } from "../../lib/utils";

interface HoldToConfirmProps {
  onConfirm: () => void;
  duration?: number; // milliseconds
  children: React.ReactNode;
  variant?: "default" | "destructive";
  className?: string;
}

export function HoldToConfirm({
  onConfirm,
  duration = 2000,
  children,
  variant = "destructive",
  className,
}: HoldToConfirmProps) {
  const [isHolding, setIsHolding] = useState(false);
  const [progress, setProgress] = useState(0);
  const holdTimerRef = useRef<NodeJS.Timeout | null>(null);
  const progressIntervalRef = useRef<NodeJS.Timeout | null>(null);

  const startHold = () => {
    setIsHolding(true);
    setProgress(0);

    // Progress animation
    const interval = 50; // Update every 50ms
    const increment = (interval / duration) * 100;

    progressIntervalRef.current = setInterval(() => {
      setProgress((prev) => {
        const next = prev + increment;
        if (next >= 100) {
          return 100;
        }
        return next;
      });
    }, interval);

    // Completion timer
    holdTimerRef.current = setTimeout(() => {
      setIsHolding(false);
      setProgress(0);
      onConfirm();
    }, duration);
  };

  const cancelHold = () => {
    if (holdTimerRef.current) {
      clearTimeout(holdTimerRef.current);
    }
    if (progressIntervalRef.current) {
      clearInterval(progressIntervalRef.current);
    }
    setIsHolding(false);
    setProgress(0);
  };

  useEffect(() => {
    return () => {
      if (holdTimerRef.current) {
        clearTimeout(holdTimerRef.current);
      }
      if (progressIntervalRef.current) {
        clearInterval(progressIntervalRef.current);
      }
    };
  }, []);

  return (
    <Button
      variant={variant}
      className={cn(
        "relative overflow-hidden",
        isHolding && "animate-pulse-glow",
        className
      )}
      onMouseDown={startHold}
      onMouseUp={cancelHold}
      onMouseLeave={cancelHold}
      onTouchStart={startHold}
      onTouchEnd={cancelHold}
    >
      <span className="relative z-10">{children}</span>
      
      {/* Progress bar */}
      {isHolding && (
        <div
          className="absolute inset-0 bg-destructive/30 transition-all duration-75"
          style={{ width: `${progress}%` }}
        />
      )}
      
      {/* Shake animation when near completion */}
      {progress > 80 && (
        <div className="absolute inset-0 animate-flicker" />
      )}
    </Button>
  );
}

