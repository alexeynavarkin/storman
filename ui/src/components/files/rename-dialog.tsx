import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { toast } from "sonner";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError, api } from "@/lib/api";

const schema = z.object({
  name: z
    .string()
    .min(1, "Required")
    .max(255, "Too long")
    .refine((v) => !v.includes("/"), "No slashes")
    .refine((v) => v !== "." && v !== "..", "Reserved name"),
});

interface RenameDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  fromPath: string;
  currentName: string;
  parentPath: string;
}

function joinPath(dir: string, name: string): string {
  const base = dir === "/" ? "" : dir;
  return base + "/" + name;
}

export function RenameDialog({
  open,
  onOpenChange,
  fromPath,
  currentName,
  parentPath,
}: RenameDialogProps) {
  const queryClient = useQueryClient();
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: { name: currentName },
  });

  useEffect(() => {
    if (open) form.reset({ name: currentName });
  }, [open, currentName, form]);

  const mutation = useMutation({
    mutationFn: (values: z.infer<typeof schema>) =>
      api<void>("/api/fs/rename", {
        method: "POST",
        json: { from: fromPath, to: joinPath(parentPath, values.name) },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["fs", "list", parentPath] });
      toast.success("Renamed");
      onOpenChange(false);
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : "Rename failed";
      form.setError("name", { message: msg });
    },
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Rename</DialogTitle>
        </DialogHeader>
        <form
          onSubmit={form.handleSubmit((v) => mutation.mutate(v))}
          className="flex flex-col gap-3"
          noValidate
        >
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="rename-name">New name</Label>
            <Input
              id="rename-name"
              autoFocus
              autoComplete="off"
              aria-invalid={!!form.formState.errors.name}
              {...form.register("name")}
            />
            {form.formState.errors.name && (
              <p className="text-xs text-destructive">
                {form.formState.errors.name.message}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="secondary"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={mutation.isPending}>
              {mutation.isPending ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                "Rename"
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
