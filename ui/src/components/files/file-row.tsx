import { Download, File, Folder, Link2, MoreHorizontal, Pencil, ShieldCheck, Trash2 } from "lucide-react";
import { Link } from "react-router";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { TableCell, TableRow } from "@/components/ui/table";
import { Action, hasAction } from "@/lib/permissions";
import { formatBytes, formatDate } from "@/lib/format";
import type { NodeInfo } from "@/types/api";

interface FileRowProps {
  node: NodeInfo;
  parentPath: string;
  routePath: string; // /files/<...> for navigation when type=dir
  onDownload: (node: NodeInfo) => void;
  onRename: (node: NodeInfo) => void;
  onDelete: (node: NodeInfo) => void;
  onPermissions: (node: NodeInfo) => void;
  onShare: (node: NodeInfo) => void;
}

export function FileRow({
  node,
  routePath,
  onDownload,
  onRename,
  onDelete,
  onPermissions,
  onShare,
}: FileRowProps) {
  const isDir = node.type === "dir";
  const canWrite = hasAction(node.effective, Action.Write);
  const canRemove = hasAction(node.effective, Action.Remove);
  const canAdmin = hasAction(node.effective, Action.Admin);
  const canRead = hasAction(node.effective, Action.Read);

  return (
    <TableRow>
      <TableCell className="w-full">
        {isDir ? (
          <Link
            to={routePath}
            className="flex items-center gap-2 text-foreground hover:underline"
          >
            <Folder className="size-4 text-muted-foreground" />
            <span className="truncate">{node.name}</span>
          </Link>
        ) : (
          <div className="flex items-center gap-2">
            <File className="size-4 text-muted-foreground" />
            <span className="truncate">{node.name}</span>
          </div>
        )}
      </TableCell>
      <TableCell className="hidden text-right text-muted-foreground sm:table-cell">
        {isDir ? "—" : formatBytes(node.size)}
      </TableCell>
      <TableCell className="hidden text-right text-muted-foreground md:table-cell">
        {formatDate(node.mtime)}
      </TableCell>
      <TableCell className="text-right">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Actions for ${node.name}`}
            >
              <MoreHorizontal className="size-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {!isDir && (
              <DropdownMenuItem
                disabled={!canRead}
                onSelect={() => onDownload(node)}
              >
                <Download className="size-4" />
                <span>Download</span>
              </DropdownMenuItem>
            )}
            <DropdownMenuItem
              disabled={!canWrite}
              onSelect={() => onRename(node)}
            >
              <Pencil className="size-4" />
              <span>Rename</span>
            </DropdownMenuItem>
            <DropdownMenuItem
              disabled={!canAdmin}
              onSelect={() => onPermissions(node)}
            >
              <ShieldCheck className="size-4" />
              <span>Permissions</span>
            </DropdownMenuItem>
            <DropdownMenuItem
              disabled={!canAdmin}
              onSelect={() => onShare(node)}
            >
              <Link2 className="size-4" />
              <span>Share link…</span>
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              disabled={!canRemove}
              onSelect={() => onDelete(node)}
              className="text-destructive focus:bg-destructive/10 focus:text-destructive"
            >
              <Trash2 className="size-4" />
              <span>Delete</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </TableCell>
    </TableRow>
  );
}
