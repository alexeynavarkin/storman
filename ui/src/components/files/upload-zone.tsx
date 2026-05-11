import { useQueryClient } from "@tanstack/react-query";
import { CloudUpload, Loader2, Upload, X } from "lucide-react";
import { useRef, useState } from "react";
import { toast } from "sonner";
import * as tus from "tus-js-client";

import { Button } from "@/components/ui/button";

interface UploadZoneProps {
  /** Absolute logical path of the destination directory (with leading slash). */
  dirPath: string;
  /** True iff the user can write to dirPath. */
  canWrite: boolean;
  /** Invalidate after each successful file upload. */
  onUploaded: () => void;
}

interface UploadTask {
  id: string;
  name: string;
  size: number;
  loaded: number;
  status: "uploading" | "done" | "error";
  error?: string;
  upload?: tus.Upload;
}

/** UploadZone renders a drop target overlay + an Upload button. Files go
 *  through the tus.io resumable endpoint at /api/tus so big uploads survive
 *  a network drop. tus-js-client transparently retries chunks and resumes
 *  from the last confirmed offset. */
export function UploadZone({ dirPath, canWrite, onUploaded }: UploadZoneProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragActive, setDragActive] = useState(false);
  const [tasks, setTasks] = useState<UploadTask[]>([]);
  const queryClient = useQueryClient();

  function startUpload(file: File) {
    const id = crypto.randomUUID();
    const newTask: UploadTask = {
      id,
      name: file.name,
      size: file.size,
      loaded: 0,
      status: "uploading",
    };
    setTasks((t) => [...t, newTask]);

    const upload = new tus.Upload(file, {
      endpoint: "/api/tus",
      // Without retries one packet drop kills the whole upload.
      retryDelays: [0, 1000, 3000, 5000, 10000],
      // Persist progress in localStorage so refresh/resume works without an
      // explicit "resume" UI. tus-js-client keys it by file fingerprint.
      removeFingerprintOnSuccess: true,
      chunkSize: 5 * 1024 * 1024,
      metadata: {
        filename: file.name,
        filetype: file.type || "application/octet-stream",
        dir: dirPath,
      },
      // Cookies (session) must travel with each request — tus-js-client
      // defaults to omit on cross-origin, but we're same-origin in prod and
      // dev-proxied through Vite, so include is the correct choice.
      onBeforeRequest: (req) => {
        req.getUnderlyingObject().withCredentials = true;
      },
      onProgress: (loaded) => {
        setTasks((all) =>
          all.map((t) => (t.id === id ? { ...t, loaded } : t)),
        );
      },
      onSuccess: () => {
        setTasks((all) =>
          all.map((t) =>
            t.id === id ? { ...t, status: "done", loaded: t.size } : t,
          ),
        );
        onUploaded();
        queryClient.invalidateQueries({ queryKey: ["fs", "list"] });
      },
      onError: (err) => {
        const msg = err instanceof Error ? err.message : String(err);
        setTasks((all) =>
          all.map((t) =>
            t.id === id ? { ...t, status: "error", error: msg } : t,
          ),
        );
        toast.error("Upload failed", { description: msg });
      },
    });

    setTasks((all) => all.map((t) => (t.id === id ? { ...t, upload } : t)));
    upload.start();
  }

  function handleFiles(files: FileList | File[] | null) {
    if (!files) return;
    for (const file of Array.from(files)) {
      startUpload(file);
    }
  }

  function cancelTask(id: string) {
    setTasks((all) => {
      const target = all.find((t) => t.id === id);
      if (target?.upload && target.status === "uploading") {
        // abort(true) tells the server to terminate (DELETE) instead of just
        // dropping the local handle — keeps meta-storage/uploads tidy.
        void target.upload.abort(true);
      }
      return all.filter((t) => t.id !== id);
    });
  }

  function onDragOver(e: React.DragEvent) {
    if (!canWrite) return;
    e.preventDefault();
    setDragActive(true);
  }
  function onDragLeave(e: React.DragEvent) {
    if (e.target === e.currentTarget) setDragActive(false);
  }
  function onDrop(e: React.DragEvent) {
    if (!canWrite) return;
    e.preventDefault();
    setDragActive(false);
    handleFiles(e.dataTransfer.files);
  }

  const activeTasks = tasks.filter((t) => t.status !== "done");

  return (
    <>
      <input
        ref={inputRef}
        type="file"
        multiple
        hidden
        onChange={(e) => {
          handleFiles(e.target.files);
          e.target.value = "";
        }}
      />
      <Button
        variant="secondary"
        size="sm"
        disabled={!canWrite}
        onClick={() => inputRef.current?.click()}
      >
        <Upload className="size-4" />
        Upload
      </Button>

      {canWrite && (
        <div
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
          className="fixed inset-0 z-30 pointer-events-none"
          aria-hidden
        >
          {dragActive && (
            <div className="pointer-events-auto absolute inset-4 flex items-center justify-center rounded-lg border-2 border-dashed border-border-strong bg-background/80 backdrop-blur-sm">
              <div className="flex flex-col items-center gap-2 text-muted-foreground">
                <CloudUpload className="size-10 text-foreground" />
                <p className="text-sm font-medium text-foreground">
                  Drop to upload to {dirPath}
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {activeTasks.length > 0 && (
        <div className="fixed bottom-4 right-4 z-50 w-80 rounded-lg border border-border bg-surface-elevated p-3 shadow-sm">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
              Uploads
            </span>
            <button
              onClick={() => {
                // Abort everything pending; tus-js-client uses DELETE to clean
                // server-side state when abort(true).
                for (const t of tasks) {
                  if (t.upload && t.status === "uploading") void t.upload.abort(true);
                }
                setTasks([]);
              }}
              className="text-muted-foreground hover:text-foreground"
              aria-label="Clear uploads"
            >
              <X className="size-3.5" />
            </button>
          </div>
          <div className="flex flex-col gap-2">
            {activeTasks.map((t) => {
              const pct = t.size > 0 ? Math.min(100, Math.round((t.loaded / t.size) * 100)) : 0;
              return (
                <div key={t.id} className="flex flex-col gap-1">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate text-sm">{t.name}</span>
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      {t.status === "uploading" && (
                        <Loader2 className="size-3 animate-spin" />
                      )}
                      <span>{pct}%</span>
                      <button
                        onClick={() => cancelTask(t.id)}
                        aria-label={`Cancel ${t.name}`}
                        className="ml-1 text-muted-foreground hover:text-foreground"
                      >
                        <X className="size-3" />
                      </button>
                    </div>
                  </div>
                  <div className="h-1 w-full overflow-hidden rounded-full bg-surface">
                    <div
                      className={
                        t.status === "error"
                          ? "h-full bg-destructive"
                          : "h-full bg-foreground transition-[width] duration-150"
                      }
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  {t.error && (
                    <p className="text-xs text-destructive">{t.error}</p>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </>
  );
}
