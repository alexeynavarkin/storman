import { useQuery } from "@tanstack/react-query";
import { KeyRound, MoreHorizontal, Trash2, UserPlus, Users as UsersIcon } from "lucide-react";
import { useState } from "react";
import { Navigate } from "react-router";

import { Header } from "@/components/layout/header";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ChangePasswordDialog } from "@/components/users/change-password-dialog";
import { DeleteUserDialog } from "@/components/users/delete-user-dialog";
import { NewUserDialog } from "@/components/users/new-user-dialog";
import { useCurrentUser } from "@/features/auth/use-current-user";
import { ApiError, api } from "@/lib/api";
import { formatDate } from "@/lib/format";
import type { UserDTO } from "@/types/api";

export function UsersRoute() {
  const me = useCurrentUser();

  // Belt-and-suspenders: the nav hides the link for non-admins, but routing
  // directly here should still bounce them.
  if (me.data && !me.data.is_root_admin) {
    return <Navigate to="/files/" replace />;
  }

  const users = useQuery({
    queryKey: ["users"],
    queryFn: () => api<UserDTO[]>("/api/users"),
  });

  const [newOpen, setNewOpen] = useState(false);
  const [pwTarget, setPwTarget] = useState<UserDTO | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<UserDTO | null>(null);

  return (
    <div className="min-h-svh bg-background">
      <Header />

      <main className="mx-auto max-w-6xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="mb-6 flex items-end justify-between">
          <div className="flex flex-col gap-0.5">
            <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
              Administration
            </span>
            <h1 className="text-2xl font-semibold tracking-tight">Users</h1>
          </div>
          <Button size="sm" onClick={() => setNewOpen(true)}>
            <UserPlus className="size-4" />
            New user
          </Button>
        </div>

        <Body
          users={users.data}
          loading={users.isLoading}
          error={users.error}
          meId={me.data?.id ?? ""}
          onChangePassword={setPwTarget}
          onDelete={setDeleteTarget}
        />
      </main>

      <NewUserDialog open={newOpen} onOpenChange={setNewOpen} />
      {pwTarget && (
        <ChangePasswordDialog
          open
          onOpenChange={(o) => !o && setPwTarget(null)}
          userId={pwTarget.id}
          login={pwTarget.login}
          isSelf={pwTarget.id === me.data?.id}
        />
      )}
      {deleteTarget && (
        <DeleteUserDialog
          open
          onOpenChange={(o) => !o && setDeleteTarget(null)}
          userId={deleteTarget.id}
          login={deleteTarget.login}
        />
      )}
    </div>
  );
}

interface BodyProps {
  users: UserDTO[] | undefined;
  loading: boolean;
  error: Error | null;
  meId: string;
  onChangePassword: (u: UserDTO) => void;
  onDelete: (u: UserDTO) => void;
}

function Body({ users, loading, error, meId, onChangePassword, onDelete }: BodyProps) {
  if (loading) {
    return (
      <div className="flex flex-col gap-1.5 rounded-md border border-border">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-9 rounded-none first:rounded-t-md last:rounded-b-md" />
        ))}
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded-md border border-dashed border-destructive/40 bg-destructive/5 p-6 text-sm text-destructive">
        {error instanceof ApiError ? error.message : "Failed to load users"}
      </div>
    );
  }
  if (!users || users.length === 0) {
    return (
      <div className="flex flex-col items-center gap-2 rounded-md border border-dashed border-border bg-surface/50 px-6 py-16 text-center">
        <div className="flex size-10 items-center justify-center rounded-md border border-border bg-surface">
          <UsersIcon className="size-6 text-muted-foreground" />
        </div>
        <h2 className="text-sm font-medium">No users yet</h2>
        <p className="max-w-md text-sm text-muted-foreground">
          Click "New user" to create the first account.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border border-border">
      <Table>
        <TableHeader>
          <TableRow className="bg-surface hover:bg-surface">
            <TableHead>Login</TableHead>
            <TableHead className="hidden text-right md:table-cell">Created</TableHead>
            <TableHead className="w-12 text-right" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {users.map((u) => (
            <TableRow key={u.id}>
              <TableCell className="w-full">
                <div className="flex items-center gap-2">
                  <span className="flex size-6 items-center justify-center rounded-md border border-border bg-surface text-[10px] font-medium">
                    {u.login.slice(0, 2).toUpperCase()}
                  </span>
                  <span className="truncate">{u.login}</span>
                  {u.id === meId && (
                    <span className="rounded border border-border bg-surface-elevated px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                      you
                    </span>
                  )}
                </div>
              </TableCell>
              <TableCell className="hidden text-right text-muted-foreground md:table-cell">
                {formatDate(u.created_at)}
              </TableCell>
              <TableCell className="text-right">
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label={`Actions for ${u.login}`}
                    >
                      <MoreHorizontal className="size-4" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onSelect={() => onChangePassword(u)}>
                      <KeyRound className="size-4" />
                      <span>{u.id === meId ? "Change my password" : "Reset password"}</span>
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem
                      disabled={u.id === meId}
                      onSelect={() => onDelete(u)}
                      className="text-destructive focus:bg-destructive/10 focus:text-destructive"
                    >
                      <Trash2 className="size-4" />
                      <span>Delete</span>
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
