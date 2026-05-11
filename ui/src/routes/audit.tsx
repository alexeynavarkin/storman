import { useQuery } from "@tanstack/react-query";
import { ShieldAlert, Activity, AlertTriangle, CheckCircle2 } from "lucide-react";
import { useMemo, useState } from "react";
import { Navigate } from "react-router";

import { Header } from "@/components/layout/header";
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
import { formatDate } from "@/lib/format";
import type { AuditRow } from "@/types/api";

const auditKey = (params: { action: string; limit: number }) =>
  ["audit", params] as const;

const ACTIONS = [
  "login",
  "login_failed",
  "logout",
  "upload",
  "delete",
  "rename",
  "mkdir",
  "trash_restore",
  "trash_purge",
  "acl_change",
  "user_create",
  "user_delete",
  "password_reset",
] as const;

export function AuditRoute() {
  const me = useCurrentUser();
  if (me.data && !me.data.is_root_admin) {
    return <Navigate to="/files/" replace />;
  }

  const [action, setAction] = useState<string>("");
  const [limit] = useState(200);

  const events = useQuery({
    queryKey: auditKey({ action, limit }),
    queryFn: () => {
      const qs = new URLSearchParams();
      if (action) qs.set("action", action);
      qs.set("limit", String(limit));
      return api<AuditRow[]>(`/api/audit?${qs.toString()}`);
    },
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
            <h1 className="text-2xl font-semibold tracking-tight">Audit log</h1>
          </div>
          <div className="flex items-center gap-2">
            <select
              value={action}
              onChange={(e) => setAction(e.target.value)}
              className="h-9 rounded-md border border-border bg-surface px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="">All actions</option>
              {ACTIONS.map((a) => (
                <option key={a} value={a}>
                  {a}
                </option>
              ))}
            </select>
            <Button variant="ghost" size="sm" onClick={() => events.refetch()}>
              Refresh
            </Button>
          </div>
        </div>

        <Body events={events.data} loading={events.isLoading} error={events.error} />
      </main>
    </div>
  );
}

interface BodyProps {
  events: AuditRow[] | undefined;
  loading: boolean;
  error: Error | null;
}

function Body({ events, loading, error }: BodyProps) {
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
        {error instanceof ApiError ? error.message : "Failed to load audit log"}
      </div>
    );
  }
  if (!events || events.length === 0) {
    return (
      <div className="flex flex-col items-center gap-2 rounded-md border border-dashed border-border bg-surface/50 px-6 py-16 text-center">
        <div className="flex size-10 items-center justify-center rounded-md border border-border bg-surface">
          <Activity className="size-6 text-muted-foreground" />
        </div>
        <h2 className="text-sm font-medium">No events match</h2>
        <p className="max-w-md text-sm text-muted-foreground">
          Try widening the filter — the log keeps entries for the last 12 months by default.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border border-border">
      <Table>
        <TableHeader>
          <TableRow className="bg-surface hover:bg-surface">
            <TableHead className="w-32">When</TableHead>
            <TableHead className="w-40">Action</TableHead>
            <TableHead className="w-24">Result</TableHead>
            <TableHead>Details</TableHead>
            <TableHead className="w-28">User</TableHead>
            <TableHead className="w-32">IP</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {events.map((e) => (
            <Row key={e.id} ev={e} />
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function Row({ ev }: { ev: AuditRow }) {
  const details = useMemo(() => formatDetails(ev.details), [ev.details]);
  return (
    <TableRow>
      <TableCell className="font-mono text-xs text-muted-foreground">{formatDate(ev.ts)}</TableCell>
      <TableCell className="font-mono text-xs">{ev.action}</TableCell>
      <TableCell>
        <ResultBadge result={ev.result} />
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{details}</TableCell>
      <TableCell className="truncate text-xs text-muted-foreground" title={ev.user_id}>
        {ev.user_id ? ev.user_id.slice(0, 8) : "—"}
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{ev.ip ?? "—"}</TableCell>
    </TableRow>
  );
}

function ResultBadge({ result }: { result: AuditRow["result"] }) {
  if (result === "ok") {
    return (
      <span className="inline-flex items-center gap-1 rounded border border-border bg-surface px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">
        <CheckCircle2 className="size-3" />
        ok
      </span>
    );
  }
  if (result === "denied") {
    return (
      <span className="inline-flex items-center gap-1 rounded border border-destructive/40 bg-destructive/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-destructive">
        <ShieldAlert className="size-3" />
        denied
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded border border-destructive/40 bg-destructive/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-destructive">
      <AlertTriangle className="size-3" />
      error
    </span>
  );
}

function formatDetails(d: Record<string, unknown> | undefined): string {
  if (!d) return "—";
  const parts: string[] = [];
  for (const [k, v] of Object.entries(d)) {
    parts.push(`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`);
  }
  return parts.join(" ");
}
