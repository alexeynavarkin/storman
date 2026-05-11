import { useMutation } from "@tanstack/react-query";
import { Copy, Check, Link2 } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Action } from "@/lib/permissions";
import { ApiError, api } from "@/lib/api";

// Match the Action bitmask in @/lib/permissions but lay out the choices the
// share dialog actually exposes — Admin and Traverse don't belong here.
const SCOPE_PRESETS = [
  { label: "View only (Read)", actions: Action.Read },
  { label: "Read + Write", actions: Action.Read | Action.Write },
  {
    label: "Read + Write + Remove",
    actions: Action.Read | Action.Write | Action.Remove,
  },
] as const;

const TTL_PRESETS = [
  { label: "1 hour", seconds: 60 * 60 },
  { label: "1 day", seconds: 24 * 60 * 60 },
  { label: "7 days", seconds: 7 * 24 * 60 * 60 },
  { label: "30 days", seconds: 30 * 24 * 60 * 60 },
] as const;

interface CreateResponse {
  token: string;
  url: string;
  expires_at: string;
  actions: number;
}

interface ShareDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  path: string;
  name: string;
  nodeType: "file" | "dir";
}

export function ShareDialog({ open, onOpenChange, path, name, nodeType }: ShareDialogProps) {
  const [scope, setScope] = useState<number>(SCOPE_PRESETS[0].actions);
  const [ttl, setTtl] = useState<number>(TTL_PRESETS[1].seconds);
  const [created, setCreated] = useState<CreateResponse | null>(null);
  const [copied, setCopied] = useState(false);

  const create = useMutation({
    mutationFn: () =>
      api<CreateResponse>("/api/share", {
        method: "POST",
        body: JSON.stringify({
          path,
          actions: scope,
          ttl_seconds: ttl,
        }),
      }),
    onSuccess: (data) => {
      setCreated(data);
    },
    onError: (err) => {
      toast.error(err instanceof ApiError ? err.message : "Could not create share link");
    },
  });

  function handleClose(open: boolean) {
    if (!open) {
      setCreated(null);
      setCopied(false);
      onOpenChange(false);
    }
  }

  // Files can't be uploaded into — strip the upload-capable presets.
  const presets = nodeType === "file"
    ? SCOPE_PRESETS.filter((p) => !(p.actions & Action.Write))
    : SCOPE_PRESETS;

  // Build the public link the operator should hand out. Browsers append no
  // suffix on /info — that's the previewer page that the SPA will (later)
  // render. For now operators can also append /download manually.
  const shareURL = created ? buildPublicURL(created.token) : "";

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Link2 className="size-4 text-muted-foreground" />
            Share {nodeType === "dir" ? "folder" : "file"}
          </DialogTitle>
          <DialogDescription>
            Anonymous link to{" "}
            <span className="rounded bg-surface px-1.5 py-0.5 font-mono text-xs">{name}</span>.
            The link expires automatically — revoke it any time from the Shares page.
          </DialogDescription>
        </DialogHeader>

        {created ? (
          <div className="flex flex-col gap-2">
            <label className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Public URL
            </label>
            <div className="flex items-center gap-2">
              <Input readOnly value={shareURL} onFocus={(e) => e.currentTarget.select()} />
              <Button
                variant="secondary"
                size="sm"
                onClick={async () => {
                  try {
                    await navigator.clipboard.writeText(shareURL);
                    setCopied(true);
                    toast.success("Copied to clipboard");
                  } catch {
                    toast.error("Clipboard unavailable — copy the URL manually");
                  }
                }}
              >
                {copied ? <Check className="size-4" /> : <Copy className="size-4" />}
                {copied ? "Copied" : "Copy"}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              Expires {new Date(created.expires_at).toLocaleString()}. Anyone with this URL gains
              the configured permissions until it expires.
            </p>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <Field label="Permission scope">
              <select
                value={String(scope)}
                onChange={(e) => setScope(Number(e.target.value))}
                className="h-9 w-full rounded-md border border-border bg-surface px-2 text-sm"
              >
                {presets.map((p) => (
                  <option key={p.label} value={p.actions}>
                    {p.label}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Valid for">
              <select
                value={String(ttl)}
                onChange={(e) => setTtl(Number(e.target.value))}
                className="h-9 w-full rounded-md border border-border bg-surface px-2 text-sm"
              >
                {TTL_PRESETS.map((p) => (
                  <option key={p.label} value={p.seconds}>
                    {p.label}
                  </option>
                ))}
              </select>
            </Field>
          </div>
        )}

        <DialogFooter>
          {created ? (
            <Button onClick={() => handleClose(false)}>Done</Button>
          ) : (
            <>
              <Button variant="ghost" onClick={() => handleClose(false)}>
                Cancel
              </Button>
              <Button onClick={() => create.mutate()} disabled={create.isPending}>
                {create.isPending ? "Creating…" : "Create link"}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      {children}
    </div>
  );
}

function buildPublicURL(token: string): string {
  return `${window.location.origin}/share/${token}/info`;
}
