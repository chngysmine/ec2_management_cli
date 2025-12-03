import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "../ui/dialog";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { AlertTriangle } from "lucide-react";

interface TypeToConfirmProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
  title: string;
  description: string;
  confirmText: string; // Text user must type
  resourceId?: string; // Optional: Instance ID or Volume ID
  variant?: "default" | "destructive";
}

export function TypeToConfirm({
  open,
  onOpenChange,
  onConfirm,
  title,
  description,
  confirmText,
  resourceId,
  variant = "destructive",
}: TypeToConfirmProps) {
  const [inputValue, setInputValue] = useState("");
  const isConfirmed = inputValue === confirmText;

  const handleConfirm = () => {
    if (isConfirmed) {
      onConfirm();
      setInputValue("");
      onOpenChange(false);
    }
  };

  const handleCancel = () => {
    setInputValue("");
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-destructive" />
            <DialogTitle>{title}</DialogTitle>
          </div>
          <DialogDescription className="pt-2">
            {description}
            {resourceId && (
              <span className="block mt-2 font-mono text-xs text-muted-foreground">
                Resource: {resourceId}
              </span>
            )}
          </DialogDescription>
        </DialogHeader>
        
        <div className="py-4">
          <label className="text-sm font-medium mb-2 block">
            Type <span className="font-mono text-destructive">{confirmText}</span> to confirm:
          </label>
          <Input
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            placeholder={confirmText}
            className="font-mono"
            autoFocus
            onKeyDown={(e) => {
              if (e.key === "Enter" && isConfirmed) {
                handleConfirm();
              }
              if (e.key === "Escape") {
                handleCancel();
              }
            }}
          />
        </div>

        <DialogFooter>
          <Button 
            variant="outline" 
            onClick={handleCancel} 
            className="border-white/20 bg-white/5 hover:bg-white/15 hover:border-white/40 hover:text-foreground hover:scale-[1.02] hover:shadow-none transition-all"
          >
            Cancel
          </Button>
          <Button
            variant={variant}
            onClick={handleConfirm}
            disabled={!isConfirmed}
            className={`min-w-[100px] hover:scale-[1.02] hover:shadow-none transition-all ${
              variant === "destructive" 
                ? "border border-transparent hover:border-white/40 hover:brightness-110" 
                : "border border-transparent hover:border-white/40 hover:brightness-110"
            }`}
          >
            Confirm
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

