import { useQuery } from "@tanstack/react-query";
import { FolderPlus, Inbox, ShieldCheck } from "lucide-react";
import { useState } from "react";
import { useLocation, useNavigate } from "react-router";
import { toast } from "sonner";

import { Breadcrumb } from "@/components/files/breadcrumb";
import { DeleteDialog } from "@/components/files/delete-dialog";
import { FileRow } from "@/components/files/file-row";
import { NewFolderDialog } from "@/components/files/new-folder-dialog";
import { PermissionsDialog } from "@/components/files/permissions-dialog";
import { RenameDialog } from "@/components/files/rename-dialog";
import { ShareDialog } from "@/components/files/share-dialog";
import { UploadZone } from "@/components/files/upload-zone";
import { ViewSettingsMenu } from "@/components/files/view-settings-menu";
import { Header } from "@/components/layout/header";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ApiError, api, rawFetch } from "@/lib/api";
import { Action, hasAction } from "@/lib/permissions";
import { useViewPrefs } from "@/lib/use-view-prefs";
import type { NodeInfo } from "@/types/api";

function pathFromLocation(pathname: string): string {
  // /files/photos/2024 → "/photos/2024", /files/ → "/"
  const rest = pathname.replace(/^\/files\/?/, "");
  if (!rest) return "/";
  return "/" + decodeURIComponent(rest).replace(/\/+$/, "");
}

function encodePath(p: string): string {
  return p
    .split("/")
    .map((seg) => (seg ? encodeURIComponent(seg) : ""))
    .join("/");
}

function routePathFor(absolute: string): string {
  if (absolute === "/") return "/files/";
  return "/files" + absolute;
}

export function FilesRoute() {
  const location = useLocation();
  const navigate = useNavigate();
  const currentPath = pathFromLocation(location.pathname);

  const statQuery = useQuery({
    queryKey: ["fs", "stat", currentPath],
    queryFn: () =>
      api<NodeInfo>(`/api/fs/stat?path=${encodePath(currentPath)}`),
  });

  // If the user navigated to a file path, redirect to the parent directory.
  // Files don't have their own view yet — they download instead.
  const stat = statQuery.data;
  if (stat && stat.type === "file") {
    const parent =
      currentPath === "/" ? "/" : currentPath.replace(/\/[^/]+$/, "") || "/";
    navigate(routePathFor(parent), { replace: true });
  }

  const listQuery = useQuery({
    queryKey: ["fs", "list", currentPath],
    queryFn: () =>
      api<NodeInfo[]>(`/api/fs/list?path=${encodePath(currentPath)}`),
    enabled: !!stat && stat.type === "dir",
  });

  const canWrite = !!stat && hasAction(stat.effective, Action.Write);
  const canAdmin = !!stat && hasAction(stat.effective, Action.Admin);

  const { showHidden, setShowHidden } = useViewPrefs();

  const [newFolderOpen, setNewFolderOpen] = useState(false);
  const [renameTarget, setRenameTarget] = useState<NodeInfo | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<NodeInfo | null>(null);
  const [permTarget, setPermTarget] = useState<NodeInfo | null>(null);
  const [shareTarget, setShareTarget] = useState<NodeInfo | null>(null);

  function handleDownload(node: NodeInfo) {
    download(node.path).catch((err) =>
      toast.error("Download failed", { description: errorMessage(err) }),
    );
  }

  return (
    <div className="min-h-svh bg-background">
      <Header />

      <main className="mx-auto max-w-6xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <Breadcrumb path={currentPath} />
          <div className="flex items-center gap-2">
            <ViewSettingsMenu
              showHidden={showHidden}
              onShowHiddenChange={setShowHidden}
            />
            <Button
              variant="secondary"
              size="sm"
              disabled={!canAdmin || !stat}
              onClick={() => stat && setPermTarget(stat)}
            >
              <ShieldCheck className="size-4" />
              Permissions
            </Button>
            <Button
              variant="secondary"
              size="sm"
              disabled={!canWrite}
              onClick={() => setNewFolderOpen(true)}
            >
              <FolderPlus className="size-4" />
              New folder
            </Button>
            <UploadZone
              dirPath={currentPath}
              canWrite={canWrite}
              onUploaded={() => listQuery.refetch()}
            />
          </div>
        </div>

        <Body
          path={currentPath}
          stat={stat}
          statError={statQuery.error}
          listQuery={listQuery}
          showHidden={showHidden}
          onShowHidden={() => setShowHidden(true)}
          onDownload={handleDownload}
          onRename={setRenameTarget}
          onDelete={setDeleteTarget}
          onPermissions={setPermTarget}
          onShare={setShareTarget}
        />
      </main>

      {stat && (
        <NewFolderDialog
          open={newFolderOpen}
          onOpenChange={setNewFolderOpen}
          parentPath={currentPath}
        />
      )}
      {renameTarget && (
        <RenameDialog
          open
          onOpenChange={(o) => !o && setRenameTarget(null)}
          fromPath={renameTarget.path}
          currentName={renameTarget.name}
          parentPath={currentPath}
        />
      )}
      {deleteTarget && (
        <DeleteDialog
          open
          onOpenChange={(o) => !o && setDeleteTarget(null)}
          path={deleteTarget.path}
          name={deleteTarget.name}
          type={deleteTarget.type}
          parentPath={currentPath}
        />
      )}
      {permTarget && (
        <PermissionsDialog
          open
          onOpenChange={(o) => !o && setPermTarget(null)}
          path={permTarget.path}
          name={permTarget.path === "/" ? "root" : permTarget.name}
        />
      )}
      {shareTarget && (
        <ShareDialog
          open
          onOpenChange={(o) => !o && setShareTarget(null)}
          path={shareTarget.path}
          name={shareTarget.name}
          nodeType={shareTarget.type}
        />
      )}
    </div>
  );
}

interface BodyProps {
  path: string;
  stat: NodeInfo | undefined;
  statError: Error | null;
  listQuery: ReturnType<typeof useQuery<NodeInfo[]>>;
  showHidden: boolean;
  onShowHidden: () => void;
  onDownload: (n: NodeInfo) => void;
  onRename: (n: NodeInfo) => void;
  onDelete: (n: NodeInfo) => void;
  onPermissions: (n: NodeInfo) => void;
  onShare: (n: NodeInfo) => void;
}

function Body({
  path,
  stat,
  statError,
  listQuery,
  showHidden,
  onShowHidden,
  onDownload,
  onRename,
  onDelete,
  onPermissions,
  onShare,
}: BodyProps) {
  if (statError) {
    return (
      <EmptyState
        title="Cannot open"
        description={errorMessage(statError)}
        icon={<Inbox className="size-6 text-muted-foreground" />}
      />
    );
  }
  if (!stat || listQuery.isLoading) {
    return (
      <div className="flex flex-col gap-1.5 rounded-md border border-border">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-9 rounded-none first:rounded-t-md last:rounded-b-md" />
        ))}
      </div>
    );
  }
  if (listQuery.error) {
    return (
      <EmptyState
        title="Cannot list"
        description={errorMessage(listQuery.error)}
        icon={<Inbox className="size-6 text-muted-foreground" />}
      />
    );
  }

  const allEntries = listQuery.data ?? [];
  const entries = showHidden
    ? allEntries
    : allEntries.filter((n) => !n.name.startsWith("."));
  if (entries.length === 0) {
    if (allEntries.length > 0) {
      return (
        <EmptyState
          title="All entries are hidden"
          description="Enable “Show hidden files” to see dot-prefixed entries."
          icon={<Inbox className="size-6 text-muted-foreground" />}
          action={
            <Button variant="secondary" size="sm" onClick={onShowHidden}>
              Show hidden files
            </Button>
          }
        />
      );
    }
    return (
      <EmptyState
        title="Empty folder"
        description="Drop files here or use the Upload button."
        icon={<Inbox className="size-6 text-muted-foreground" />}
      />
    );
  }

  return (
    <div className="overflow-hidden rounded-md border border-border">
      <Table>
        <TableHeader>
          <TableRow className="bg-surface hover:bg-surface">
            <TableHead>Name</TableHead>
            <TableHead className="hidden text-right sm:table-cell">Size</TableHead>
            <TableHead className="hidden text-right md:table-cell">Modified</TableHead>
            <TableHead className="w-12 text-right" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {entries.map((node) => (
            <FileRow
              key={node.id}
              node={node}
              parentPath={path}
              routePath={routePathFor(node.path)}
              onDownload={onDownload}
              onRename={onRename}
              onDelete={onDelete}
              onPermissions={onPermissions}
              onShare={onShare}
            />
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function EmptyState({
  title,
  description,
  icon,
  action,
}: {
  title: string;
  description: string;
  icon: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-md border border-dashed border-border bg-surface/50 px-6 py-16 text-center">
      <div className="flex size-10 items-center justify-center rounded-md border border-border bg-surface">
        {icon}
      </div>
      <h2 className="text-sm font-medium">{title}</h2>
      <p className="max-w-md text-sm text-muted-foreground">{description}</p>
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

async function download(path: string): Promise<void> {
  const res = await rawFetch(`/api/fs/read?path=${encodePath(path)}`);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  const name = path.split("/").pop() ?? "download";
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function errorMessage(err: unknown): string {
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  return "Unknown error";
}
