import { ChevronRight, HardDrive } from "lucide-react";
import { Link } from "react-router";

import { cn } from "@/lib/cn";

interface BreadcrumbProps {
  /** Absolute logical path: "/", "/photos", "/photos/2024" */
  path: string;
}

export function Breadcrumb({ path }: BreadcrumbProps) {
  const segments = path === "/" ? [] : path.replace(/^\/+/, "").split("/");

  return (
    <nav
      aria-label="Breadcrumb"
      className="flex items-center gap-1 text-sm text-muted-foreground"
    >
      <Link
        to="/files/"
        className={cn(
          "flex items-center gap-1.5 rounded-md px-1.5 py-1 hover:bg-surface-hover hover:text-foreground",
          segments.length === 0 && "text-foreground",
        )}
      >
        <HardDrive className="size-3.5" />
        <span>storman</span>
      </Link>
      {segments.map((seg, i) => {
        const target = "/files/" + segments.slice(0, i + 1).join("/");
        const isLast = i === segments.length - 1;
        return (
          <span key={target} className="flex items-center gap-1">
            <ChevronRight className="size-3.5 text-faint" />
            <Link
              to={target}
              className={cn(
                "rounded-md px-1.5 py-1 hover:bg-surface-hover hover:text-foreground",
                isLast && "text-foreground",
              )}
            >
              {seg}
            </Link>
          </span>
        );
      })}
    </nav>
  );
}
