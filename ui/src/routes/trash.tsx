import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Trash2, Undo2, AlertTriangle } from "lucide-react";
import { useState } from "react";
import { Navigate } from "react-router";
import { toast } from "sonner";

import { Header } from "@/components/layout/header";
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
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useCurrentUser } from "@/features/auth/use-current-user";
import { ApiError, api } from "@/lib/api";
import { formatDate, formatSize } from "@/lib/format";
import type { TrashEntry } from "@/types/api";

const trashKey = ["trash"] as const;

export function TrashRoute() {
  const me = useCurrentUser();
  if (me.data && !me.data.is_root_admin) {
    return <Navigate to="/files/" replace />;
  }

  const queryClient = useQueryClient();
  const trash = useQuery({
    queryKey: trashKey,
    queryFn: () => api<TrashEntry[]>("/api/trash"),
  });

  const [purgeTarget, setPurgeTarget] = useState<TrashEntry | null>(null);

  const restore = useMutation({
    mutationFn: (id: string) =>
      api<void>(`/api/trash/${id}/restore`, { method: "POST" }),
    onSuccess: () => {
      toast.success("Entry restored");
      void queryClient.invalidateQueries({ queryKey: trashKey });
    },
    onError: (err) => toast.error(err instanceof ApiError ? err.message : "Restore failed"),
  });

  const purge = useMutation({
    mutationFn: (id: string) => api<void>(`/api/trash/${id}`, { method: "DELETE" }),
    onSuccess: () => {
      toast.success("Entry deleted permanently");
      setPurgeTarget(null);
      void queryClient.invalidateQueries({ queryKey: trashKey });
    },
    onError: (err) => toast.error(err instanceof ApiError ? err.message : "Purge failed"),
  });

  return (
    <div className="min-h-svh bg-background">
      <Header />

      <main className="mx-auto max-w-6xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="mb-6 flex items-end justify-between">
          <div className="flex flex-col gap-0.5">
            <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
              Administration
            </span>
            <h1 className="text-2xl font-semibold tracking-tight">Trash</h1>
          </div>
        </div>

        <Body
          entries={trash.data}
          loading={trash.isLoading}
          error={trash.error}
          onRestore={(e) => restore.mutate(e.trash_uuid)}
          onPurge={setPurgeTarget}
          busyId={restore.isPending ? restore.variables : undefined}
        />
      </main>

      <AlertDialog open={purgeTarget !== null} onOpenChange={(o) => !o && setPurgeTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete permanently?</AlertDialogTitle>
            <AlertDialogDescription>
              <span className="block">
                The subtree at{" "}
                <span className="rounded bg-surface px-1.5 py-0.5 font-mono text-xs">
                  {purgeTarget?.root_logical_path}
                </span>{" "}
                will be deleted permanently. Both the metadata in the database and the on-disk
                content are removed — this cannot be undone.
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => purgeTarget && purge.mutate(purgeTarget.trash_uuid)}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete permanently
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

interface BodyProps {
  entries: TrashEntry[] | undefined;
  loading: boolean;
  error: Error | null;
  onRestore: (e: TrashEntry) => void;
  onPurge: (e: TrashEntry) => void;
  busyId?: string | undefined;
}

function Body({ entries, loading, error, onRestore, onPurge, busyId }: BodyProps) {
  if (loading) {
    return (
      <div className="flex flex-col gap-1.5 rounded-md border border-border">
        {[0, 1, 2].map((i) => (
          <Skeleton key={i} className="h-9 rounded-none first:rounded-t-md last:rounded-b-md" />
        ))}
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded-md border border-dashed border-destructive/40 bg-destructive/5 p-6 text-sm text-destructive">
        {error instanceof ApiError ? error.message : "Failed to load trash"}
      </div>
    );
  }
  if (!entries || entries.length === 0) {
    return (
      <div className="flex flex-col items-center gap-2 rounded-md border border-dashed border-border bg-surface/50 px-6 py-16 text-center">
        <div className="flex size-10 items-center justify-center rounded-md border border-border bg-surface">
          <Trash2 className="size-6 text-muted-foreground" />
        </div>
        <h2 className="text-sm font-medium">Trash is empty</h2>
        <p className="max-w-md text-sm text-muted-foreground">
          Deleted files and folders land here. They are purged automatically after the retention
          period passes.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border border-border">
      <Table>
        <TableHeader>
          <TableRow className="bg-surface hover:bg-surface">
            <TableHead>Original path</TableHead>
            <TableHead className="hidden md:table-cell">Type</TableHead>
            <TableHead className="hidden text-right md:table-cell">Size</TableHead>
            <TableHead className="hidden text-right md:table-cell">Deleted</TableHead>
            <TableHead className="w-40 text-right" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {entries.map((e) => (
            <TableRow key={e.trash_uuid}>
              <TableCell className="font-mono text-xs">{e.root_logical_path}</TableCell>
              <TableCell className="hidden text-muted-foreground md:table-cell">
                {e.root_node_type}
              </TableCell>
              <TableCell className="hidden text-right text-muted-foreground md:table-cell">
                {formatSize(e.size_bytes)}
              </TableCell>
              <TableCell className="hidden text-right text-muted-foreground md:table-cell">
                {formatDate(e.deleted_at)}
              </TableCell>
              <TableCell className="text-right">
                <div className="flex items-center justify-end gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={!e.can_restore || busyId === e.trash_uuid}
                    onClick={() => onRestore(e)}
                  >
                    <Undo2 className="size-3.5" />
                    Restore
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={!e.can_purge}
                    onClick={() => onPurge(e)}
                    className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                  >
                    <AlertTriangle className="size-3.5" />
                    Purge
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
