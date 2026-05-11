import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, Plus, Trash2, UserPlus } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/lib/api";
import {
  Action,
  STORABLE_ACTIONS,
  hasAction,
  labelFor,
  setAction,
} from "@/lib/permissions";
import type { PermListResponse, UserDTO } from "@/types/api";

interface PermissionsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Full absolute path of the node being edited. */
  path: string;
  name: string;
}

export function PermissionsDialog({
  open,
  onOpenChange,
  path,
  name,
}: PermissionsDialogProps) {
  const queryClient = useQueryClient();

  const list = useQuery({
    queryKey: ["perm", "list", path],
    queryFn: () =>
      api<PermListResponse>(`/api/perm/list?path=${encodeURIComponent(path)}`),
    enabled: open,
  });

  const users = useQuery({
    queryKey: ["users"],
    queryFn: () => api<UserDTO[]>("/api/users"),
    enabled: open,
    staleTime: 5 * 60_000,
  });

  const grant = useMutation({
    mutationFn: (vars: { userId: string; actions: number }) =>
      api<void>("/api/perm/grant", {
        method: "POST",
        json: { path, user_id: vars.userId, actions: vars.actions },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["perm", "list", path] });
    },
    onError: (err) => {
      toast.error("Could not update permissions", { description: err.message });
    },
  });

  const revoke = useMutation({
    mutationFn: (userId: string) =>
      api<void>("/api/perm/revoke", {
        method: "POST",
        json: { path, user_id: userId },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["perm", "list", path] });
    },
    onError: (err) => {
      toast.error("Could not revoke", { description: err.message });
    },
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Permissions</DialogTitle>
          <DialogDescription>
            Explicit grants on{" "}
            <span className="font-medium text-foreground">{name}</span>.
            Inherited from ancestors when empty.
          </DialogDescription>
        </DialogHeader>

        {list.isLoading ? (
          <div className="flex flex-col gap-2">
            <Skeleton className="h-9" />
            <Skeleton className="h-9" />
          </div>
        ) : list.error ? (
          <p className="text-sm text-destructive">{list.error.message}</p>
        ) : list.data ? (
          <PermEditor
            data={list.data}
            users={users.data ?? []}
            usersLoading={users.isLoading}
            onChange={(userId, mask) =>
              grant.mutate({ userId, actions: mask })
            }
            onRevoke={(userId) => revoke.mutate(userId)}
            pendingUserId={
              grant.isPending && typeof grant.variables === "object"
                ? grant.variables.userId
                : revoke.isPending && typeof revoke.variables === "string"
                  ? revoke.variables
                  : null
            }
          />
        ) : null}
      </DialogContent>
    </Dialog>
  );
}

interface PermEditorProps {
  data: PermListResponse;
  users: UserDTO[];
  usersLoading: boolean;
  onChange: (userId: string, mask: number) => void;
  onRevoke: (userId: string) => void;
  pendingUserId: string | null;
}

function PermEditor({
  data,
  users,
  usersLoading,
  onChange,
  onRevoke,
  pendingUserId,
}: PermEditorProps) {
  const explicitUserIds = new Set(data.explicit.map((e) => e.user_id));
  const candidates = users.filter((u) => !explicitUserIds.has(u.id));

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-hidden rounded-md border border-border">
        <div className="grid grid-cols-[1fr_repeat(4,auto)_auto] items-center gap-x-4 border-b border-border bg-surface px-3 py-2 text-xs font-medium tracking-wider uppercase text-muted-foreground">
          <span>User</span>
          {STORABLE_ACTIONS.map((a) => (
            <span key={a} className="w-12 text-center">
              {labelFor(a)}
            </span>
          ))}
          <span className="sr-only">Actions</span>
        </div>

        {data.explicit.length === 0 ? (
          <div className="px-3 py-6 text-center text-sm text-muted-foreground">
            No explicit grants. Access is inherited from ancestor folders.
          </div>
        ) : (
          data.explicit.map((entry) => (
            <PermRow
              key={entry.user_id}
              login={entry.login}
              mask={entry.actions}
              pending={pendingUserId === entry.user_id}
              onChange={(mask) => onChange(entry.user_id, mask)}
              onRevoke={() => onRevoke(entry.user_id)}
            />
          ))
        )}
      </div>

      <AddUserCombobox
        candidates={candidates}
        loading={usersLoading}
        onPick={(userId) => onChange(userId, Action.Read)}
        pending={!!pendingUserId}
      />
    </div>
  );
}

interface PermRowProps {
  login: string;
  mask: number;
  pending: boolean;
  onChange: (mask: number) => void;
  onRevoke: () => void;
}

function PermRow({ login, mask, pending, onChange, onRevoke }: PermRowProps) {
  return (
    <div className="grid grid-cols-[1fr_repeat(4,auto)_auto] items-center gap-x-4 border-b border-border px-3 py-2 last:border-b-0">
      <div className="flex items-center gap-2 min-w-0">
        <span className="flex size-6 shrink-0 items-center justify-center rounded-md border border-border bg-surface text-[10px] font-medium">
          {login.slice(0, 2).toUpperCase()}
        </span>
        <span className="truncate text-sm">{login}</span>
        {pending && <Loader2 className="size-3 animate-spin text-muted-foreground" />}
      </div>
      {STORABLE_ACTIONS.map((action) => (
        <div key={action} className="flex w-12 justify-center">
          <Checkbox
            aria-label={`${labelFor(action)} for ${login}`}
            checked={hasAction(mask, action)}
            disabled={pending}
            onCheckedChange={(checked) => {
              const next = setAction(mask, action, checked === true);
              if (next === 0) {
                onRevoke();
              } else {
                onChange(next);
              }
            }}
          />
        </div>
      ))}
      <Button
        variant="ghost"
        size="icon"
        aria-label={`Revoke ${login}`}
        onClick={onRevoke}
        disabled={pending}
      >
        <Trash2 className="size-4 text-muted-foreground" />
      </Button>
    </div>
  );
}

interface AddUserComboboxProps {
  candidates: UserDTO[];
  loading: boolean;
  onPick: (userId: string) => void;
  pending: boolean;
}

function AddUserCombobox({
  candidates,
  loading,
  onPick,
  pending,
}: AddUserComboboxProps) {
  const [open, setOpen] = useState(false);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="secondary" size="sm" disabled={pending}>
          <UserPlus className="size-4" />
          Add user
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-0">
        <Command>
          <CommandInput placeholder="Search users…" />
          <CommandList>
            {loading && (
              <div className="px-3 py-4 text-sm text-muted-foreground">
                Loading…
              </div>
            )}
            {!loading && <CommandEmpty>No users.</CommandEmpty>}
            <CommandGroup>
              {candidates.map((u) => (
                <CommandItem
                  key={u.id}
                  value={u.login}
                  onSelect={() => {
                    setOpen(false);
                    onPick(u.id);
                  }}
                >
                  <span className="flex size-6 items-center justify-center rounded-md border border-border bg-surface text-[10px] font-medium">
                    {u.login.slice(0, 2).toUpperCase()}
                  </span>
                  <span className="text-sm">{u.login}</span>
                  <Badge variant="muted" className="ml-auto">
                    <Plus className="size-3" /> Read
                  </Badge>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
