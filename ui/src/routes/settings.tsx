import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, Loader2 } from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";

import { Header } from "@/components/layout/header";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useCurrentUser } from "@/features/auth/use-current-user";
import { ApiError, api } from "@/lib/api";

// Shape mirrors backend config.Config + envelope wrapper. We keep it loose
// (any-typed) — the backend is the source of truth and we round-trip JSON
// without per-field types because the form has 30+ fields across nested
// objects.
type ConfigEnvelope = {
  config: AppConfig;
  secrets_key_present: boolean;
  restart_pending: boolean;
};

type AppConfig = {
  data_dir: string;
  database: { dsn: string };
  secrets_key: string;
  backup: {
    interval: string;
    retention: number;
    pg_dump_cmd?: string[] | null;
    pg_restore_cmd?: string[] | null;
  };
  web: {
    listen_addr: string;
    tls: { cert_file: string; key_file: string };
    secure_cookies: boolean;
    trust_proxy_headers: boolean;
  };
  trash: { retention_days: number; gc_interval: string };
  ftp: {
    enabled: boolean;
    listen_addr: string;
    public_host?: string;
    passive_port_min: number;
    passive_port_max: number;
    idle_timeout_sec: number;
    tls: { cert_file: string; key_file: string };
  };
  indexing: { workers: number; poll_interval: string };
  tus: { retention_hours: number; sweep_interval: string };
  webdav: { enabled: boolean; path_prefix?: string };
  webauthn: {
    rp_id: string;
    rp_display_name?: string;
    rp_origins?: string[];
    challenge_ttl_sec?: number;
    sweep_interval_sec?: number;
  };
};

export const settingsQueryKey = ["admin", "config"] as const;

export function SettingsRoute() {
  const me = useCurrentUser();
  const queryClient = useQueryClient();

  const envelope = useQuery({
    queryKey: settingsQueryKey,
    queryFn: () => api<ConfigEnvelope>("/api/admin/config"),
    enabled: me.data?.is_root_admin ?? false,
  });

  const [draft, setDraft] = useState<AppConfig | null>(null);
  useEffect(() => {
    if (envelope.data) setDraft(envelope.data.config);
  }, [envelope.data]);

  const save = useMutation({
    mutationFn: (cfg: AppConfig) =>
      api<ConfigEnvelope>("/api/admin/config", { method: "PUT", json: cfg }),
    onSuccess: (data) => {
      queryClient.setQueryData(settingsQueryKey, data);
      setDraft(data.config);
      toast.success("Settings saved — restart the server to apply");
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : "Save failed";
      toast.error(msg);
    },
  });

  if (!me.data?.is_root_admin) {
    return (
      <>
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10">
          <p className="text-sm text-muted-foreground">Admin access required.</p>
        </main>
      </>
    );
  }

  if (envelope.isLoading || !draft) {
    return (
      <>
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10">
          <Loader2 className="size-4 animate-spin" />
        </main>
      </>
    );
  }

  const restartPending = envelope.data?.restart_pending ?? false;

  return (
    <>
      <Header />
      <main className="mx-auto max-w-3xl px-6 py-8">
        <div className="mb-6 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
            <p className="text-sm text-muted-foreground">
              Edits write to <code className="text-xs">config.json</code>. Changes
              take effect on the next server restart.
            </p>
          </div>
          <Button onClick={() => save.mutate(draft)} disabled={save.isPending}>
            {save.isPending ? <Loader2 className="size-4 animate-spin" /> : "Save"}
          </Button>
        </div>

        {restartPending && (
          <div className="mb-6 flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm">
            <AlertCircle className="mt-0.5 size-4 shrink-0 text-amber-600" />
            <div>
              <p className="font-medium">Restart pending</p>
              <p className="text-muted-foreground">
                Recent changes are saved to disk but not yet applied. Restart the
                storman process for the new settings to take effect.
              </p>
            </div>
          </div>
        )}

        <div className="flex flex-col gap-8">
          <Section title="System (read-only)" description="Intrinsic to the deployment — change via CLI/filesystem only.">
            <Field label="Data directory">
              <Input value={draft.data_dir} readOnly disabled />
            </Field>
            <Field label="Secrets key">
              <Input
                value={envelope.data?.secrets_key_present ? "configured" : "missing"}
                readOnly
                disabled
              />
            </Field>
          </Section>

          <Section title="Database">
            <Field label="DSN" hint="postgres://user:pass@host:5432/db?sslmode=disable">
              <Input
                value={draft.database.dsn}
                onChange={(e) => setDraft({ ...draft, database: { dsn: e.target.value } })}
              />
            </Field>
          </Section>

          <Section title="Web server">
            <Field label="Listen address">
              <Input
                value={draft.web.listen_addr}
                onChange={(e) => setDraft({ ...draft, web: { ...draft.web, listen_addr: e.target.value } })}
              />
            </Field>
            <Field label="TLS cert file">
              <Input
                value={draft.web.tls.cert_file}
                onChange={(e) =>
                  setDraft({ ...draft, web: { ...draft.web, tls: { ...draft.web.tls, cert_file: e.target.value } } })
                }
              />
            </Field>
            <Field label="TLS key file">
              <Input
                value={draft.web.tls.key_file}
                onChange={(e) =>
                  setDraft({ ...draft, web: { ...draft.web, tls: { ...draft.web.tls, key_file: e.target.value } } })
                }
              />
            </Field>
            <Toggle
              label="Secure cookies"
              hint="Set Secure flag on session/CSRF cookies."
              value={draft.web.secure_cookies}
              onChange={(v) => setDraft({ ...draft, web: { ...draft.web, secure_cookies: v } })}
            />
            <Toggle
              label="Trust proxy headers"
              hint="Honour X-Forwarded-Proto. Only safe behind a trusted proxy."
              value={draft.web.trust_proxy_headers}
              onChange={(v) => setDraft({ ...draft, web: { ...draft.web, trust_proxy_headers: v } })}
            />
          </Section>

          <Section title="Passkeys (WebAuthn)" description="Empty Relying Party ID disables passkeys entirely.">
            <Field label="Relying Party ID" hint='Bare domain, e.g. "files.example.com"'>
              <Input
                value={draft.webauthn.rp_id}
                onChange={(e) => setDraft({ ...draft, webauthn: { ...draft.webauthn, rp_id: e.target.value } })}
              />
            </Field>
            <Field label="Display name">
              <Input
                value={draft.webauthn.rp_display_name ?? ""}
                onChange={(e) => setDraft({ ...draft, webauthn: { ...draft.webauthn, rp_display_name: e.target.value } })}
              />
            </Field>
            <Field label="Origins" hint="Comma-separated. https://files.example.com">
              <Input
                value={(draft.webauthn.rp_origins ?? []).join(", ")}
                onChange={(e) =>
                  setDraft({
                    ...draft,
                    webauthn: {
                      ...draft.webauthn,
                      rp_origins: e.target.value
                        .split(",")
                        .map((s) => s.trim())
                        .filter(Boolean),
                    },
                  })
                }
              />
            </Field>
            <Field label="Challenge TTL (seconds)">
              <NumInput
                value={draft.webauthn.challenge_ttl_sec ?? 0}
                onChange={(v) => setDraft({ ...draft, webauthn: { ...draft.webauthn, challenge_ttl_sec: v } })}
              />
            </Field>
            <Field label="Sweep interval (seconds)">
              <NumInput
                value={draft.webauthn.sweep_interval_sec ?? 0}
                onChange={(v) => setDraft({ ...draft, webauthn: { ...draft.webauthn, sweep_interval_sec: v } })}
              />
            </Field>
          </Section>

          <Section title="Trash">
            <Field label="Retention days" hint="0 disables time-based GC.">
              <NumInput
                value={draft.trash.retention_days}
                onChange={(v) => setDraft({ ...draft, trash: { ...draft.trash, retention_days: v } })}
              />
            </Field>
            <Field label="GC interval" hint="Go duration: 1h, 30m, 15s">
              <Input
                value={draft.trash.gc_interval}
                onChange={(e) => setDraft({ ...draft, trash: { ...draft.trash, gc_interval: e.target.value } })}
              />
            </Field>
          </Section>

          <Section title="Backup">
            <Field label="Interval">
              <Input
                value={draft.backup.interval}
                onChange={(e) => setDraft({ ...draft, backup: { ...draft.backup, interval: e.target.value } })}
              />
            </Field>
            <Field label="Retention (number of archives)">
              <NumInput
                value={draft.backup.retention}
                onChange={(v) => setDraft({ ...draft, backup: { ...draft.backup, retention: v } })}
              />
            </Field>
            <Field label="pg_dump command" hint="Space-separated argv. Empty for default.">
              <Input
                value={(draft.backup.pg_dump_cmd ?? []).join(" ")}
                onChange={(e) => setDraft({ ...draft, backup: { ...draft.backup, pg_dump_cmd: splitArgv(e.target.value) } })}
              />
            </Field>
            <Field label="pg_restore command">
              <Input
                value={(draft.backup.pg_restore_cmd ?? []).join(" ")}
                onChange={(e) => setDraft({ ...draft, backup: { ...draft.backup, pg_restore_cmd: splitArgv(e.target.value) } })}
              />
            </Field>
          </Section>

          <Section title="FTP">
            <Toggle
              label="Enabled"
              value={draft.ftp.enabled}
              onChange={(v) => setDraft({ ...draft, ftp: { ...draft.ftp, enabled: v } })}
            />
            <Field label="Listen address">
              <Input
                value={draft.ftp.listen_addr}
                onChange={(e) => setDraft({ ...draft, ftp: { ...draft.ftp, listen_addr: e.target.value } })}
              />
            </Field>
            <Field label="Public host (for PASV)">
              <Input
                value={draft.ftp.public_host ?? ""}
                onChange={(e) => setDraft({ ...draft, ftp: { ...draft.ftp, public_host: e.target.value } })}
              />
            </Field>
            <Field label="Passive port min">
              <NumInput
                value={draft.ftp.passive_port_min}
                onChange={(v) => setDraft({ ...draft, ftp: { ...draft.ftp, passive_port_min: v } })}
              />
            </Field>
            <Field label="Passive port max">
              <NumInput
                value={draft.ftp.passive_port_max}
                onChange={(v) => setDraft({ ...draft, ftp: { ...draft.ftp, passive_port_max: v } })}
              />
            </Field>
            <Field label="Idle timeout (seconds)">
              <NumInput
                value={draft.ftp.idle_timeout_sec}
                onChange={(v) => setDraft({ ...draft, ftp: { ...draft.ftp, idle_timeout_sec: v } })}
              />
            </Field>
            <Field label="TLS cert file (overrides web TLS)">
              <Input
                value={draft.ftp.tls.cert_file}
                onChange={(e) =>
                  setDraft({ ...draft, ftp: { ...draft.ftp, tls: { ...draft.ftp.tls, cert_file: e.target.value } } })
                }
              />
            </Field>
            <Field label="TLS key file">
              <Input
                value={draft.ftp.tls.key_file}
                onChange={(e) =>
                  setDraft({ ...draft, ftp: { ...draft.ftp, tls: { ...draft.ftp.tls, key_file: e.target.value } } })
                }
              />
            </Field>
          </Section>

          <Section title="WebDAV">
            <Toggle
              label="Enabled"
              value={draft.webdav.enabled}
              onChange={(v) => setDraft({ ...draft, webdav: { ...draft.webdav, enabled: v } })}
            />
            <Field label="Path prefix" hint="e.g. /dav">
              <Input
                value={draft.webdav.path_prefix ?? ""}
                onChange={(e) => setDraft({ ...draft, webdav: { ...draft.webdav, path_prefix: e.target.value } })}
              />
            </Field>
          </Section>

          <Section title="Indexing (background jobs)">
            <Field label="Workers">
              <NumInput
                value={draft.indexing.workers}
                onChange={(v) => setDraft({ ...draft, indexing: { ...draft.indexing, workers: v } })}
              />
            </Field>
            <Field label="Poll interval">
              <Input
                value={draft.indexing.poll_interval}
                onChange={(e) => setDraft({ ...draft, indexing: { ...draft.indexing, poll_interval: e.target.value } })}
              />
            </Field>
          </Section>

          <Section title="Tus uploads">
            <Field label="Retention hours">
              <NumInput
                value={draft.tus.retention_hours}
                onChange={(v) => setDraft({ ...draft, tus: { ...draft.tus, retention_hours: v } })}
              />
            </Field>
            <Field label="Sweep interval">
              <Input
                value={draft.tus.sweep_interval}
                onChange={(e) => setDraft({ ...draft, tus: { ...draft.tus, sweep_interval: e.target.value } })}
              />
            </Field>
          </Section>
        </div>

        <div className="mt-8 flex justify-end gap-2">
          <Button
            variant="secondary"
            onClick={() => envelope.data && setDraft(envelope.data.config)}
            disabled={save.isPending}
          >
            Reset
          </Button>
          <Button onClick={() => save.mutate(draft)} disabled={save.isPending}>
            {save.isPending ? <Loader2 className="size-4 animate-spin" /> : "Save"}
          </Button>
        </div>
      </main>
    </>
  );
}

function Section({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="flex flex-col gap-3">
      <div>
        <h2 className="text-base font-medium">{title}</h2>
        {description && <p className="text-xs text-muted-foreground">{description}</p>}
      </div>
      <div className="flex flex-col gap-3 rounded-md border border-border p-4">{children}</div>
    </section>
  );
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1">
      <Label>{label}</Label>
      {children}
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

function Toggle({
  label,
  hint,
  value,
  onChange,
}: {
  label: string;
  hint?: string;
  value: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label className="flex items-start gap-2 text-sm">
      <input
        type="checkbox"
        checked={value}
        onChange={(e) => onChange(e.target.checked)}
        className="mt-1"
      />
      <span className="flex flex-col">
        <span>{label}</span>
        {hint && <span className="text-xs text-muted-foreground">{hint}</span>}
      </span>
    </label>
  );
}

function NumInput({ value, onChange }: { value: number; onChange: (v: number) => void }) {
  return (
    <Input
      type="number"
      value={Number.isFinite(value) ? value : 0}
      onChange={(e) => {
        const n = Number(e.target.value);
        if (Number.isFinite(n)) onChange(n);
      }}
    />
  );
}

function splitArgv(s: string): string[] | null {
  const parts = s.trim().split(/\s+/).filter(Boolean);
  return parts.length > 0 ? parts : null;
}
