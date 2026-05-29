import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Fingerprint, KeyRound, LogOut } from "lucide-react";
import type { ReactNode } from "react";
import { useState } from "react";
import { NavLink, useNavigate } from "react-router";

import { ChangePasswordDialog } from "@/components/users/change-password-dialog";
import { PasskeysDialog } from "@/features/passkeys/passkeys-dialog";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { currentUserQueryKey, useCurrentUser } from "@/features/auth/use-current-user";
import { api } from "@/lib/api";
import { cn } from "@/lib/cn";

export function Header() {
  const me = useCurrentUser();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [pwOpen, setPwOpen] = useState(false);
  const [pkOpen, setPkOpen] = useState(false);

  const logout = useMutation({
    mutationFn: () => api<void>("/api/auth/logout", { method: "POST" }),
    onSuccess: () => {
      queryClient.setQueryData(currentUserQueryKey, null);
      queryClient.clear();
      navigate("/login", { replace: true });
    },
  });

  const initials = me.data?.login.slice(0, 2).toUpperCase() ?? "··";
  const isAdmin = me.data?.is_root_admin ?? false;

  return (
    <header className="sticky top-0 z-40 flex h-12 items-center justify-between border-b border-border bg-background/80 px-6 backdrop-blur">
      <div className="flex items-center gap-6">
        <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
          storman
        </span>
        <nav className="flex items-center gap-1">
          <NavTab to="/files/">Files</NavTab>
          {isAdmin && (
            <>
              <NavTab to="/users" exact>
                Users
              </NavTab>
              <NavTab to="/trash" exact>
                Trash
              </NavTab>
              <NavTab to="/audit" exact>
                Audit
              </NavTab>
              <NavTab to="/settings" exact>
                Settings
              </NavTab>
            </>
          )}
        </nav>
      </div>

      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            className="gap-2 px-2 text-foreground"
            aria-label="Account menu"
          >
            <span className="flex size-6 items-center justify-center rounded-md border border-border bg-surface text-[10px] font-medium">
              {initials}
            </span>
            <span className="text-sm">{me.data?.login ?? "—"}</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="min-w-[12rem]">
          <DropdownMenuLabel>Signed in</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem onSelect={() => setPwOpen(true)}>
            <KeyRound className="size-4" />
            <span>Change password</span>
          </DropdownMenuItem>
          {me.data?.passkeys_enabled && (
            <DropdownMenuItem onSelect={() => setPkOpen(true)}>
              <Fingerprint className="size-4" />
              <span>Manage passkeys</span>
            </DropdownMenuItem>
          )}
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onSelect={(e) => {
              e.preventDefault();
              logout.mutate();
            }}
          >
            <LogOut className="size-4" />
            <span>Log out</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      {me.data && (
        <ChangePasswordDialog
          open={pwOpen}
          onOpenChange={setPwOpen}
          userId={me.data.id}
          login={me.data.login}
          isSelf
        />
      )}
      {me.data?.passkeys_enabled && (
        <PasskeysDialog open={pkOpen} onOpenChange={setPkOpen} />
      )}
    </header>
  );
}

interface NavTabProps {
  to: string;
  children: ReactNode;
  /** When true the link is only active on an exact path match. */
  exact?: boolean;
}

function NavTab({ to, children, exact }: NavTabProps) {
  return (
    <NavLink
      to={to}
      end={exact}
      className={({ isActive }) =>
        cn(
          "rounded-md px-2 py-1 text-sm transition-colors",
          isActive
            ? "text-foreground bg-surface-hover"
            : "text-muted-foreground hover:text-foreground hover:bg-surface-hover",
        )
      }
    >
      {children}
    </NavLink>
  );
}
