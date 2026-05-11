import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { toast } from "sonner";

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { ApiError, api } from "@/lib/api";

interface DeleteDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  path: string;
  name: string;
  type: "file" | "dir";
  parentPath: string;
}

function encodePath(p: string): string {
  return p
    .split("/")
    .map((seg) => (seg ? encodeURIComponent(seg) : ""))
    .join("/");
}

export function DeleteDialog({
  open,
  onOpenChange,
  path,
  name,
  type,
  parentPath,
}: DeleteDialogProps) {
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: () =>
      api<void>(`/api/fs/remove?path=${encodePath(path)}`, { method: "DELETE" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["fs", "list", parentPath] });
      toast.success(`${type === "dir" ? "Folder" : "File"} moved to trash`);
      onOpenChange(false);
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : "Delete failed";
      toast.error("Could not delete", { description: msg });
    },
  });

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>
            {type === "dir" ? "Delete folder?" : "Delete file?"}
          </AlertDialogTitle>
          <AlertDialogDescription>
            <span className="text-foreground font-medium">{name}</span> will be moved
            to trash. {type === "dir" && "The folder must be empty."}
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            onClick={(e) => {
              e.preventDefault();
              mutation.mutate();
            }}
            disabled={mutation.isPending}
          >
            {mutation.isPending ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              `Delete ${type === "dir" ? "folder" : "file"}`
            )}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
