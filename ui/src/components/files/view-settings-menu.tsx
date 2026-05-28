import { SlidersHorizontal } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface ViewSettingsMenuProps {
  showHidden: boolean;
  onShowHiddenChange: (value: boolean) => void;
}

export function ViewSettingsMenu({
  showHidden,
  onShowHiddenChange,
}: ViewSettingsMenuProps) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="secondary" size="sm" aria-label="View settings">
          <SlidersHorizontal className="size-4" />
          View
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-[14rem]">
        <DropdownMenuLabel>View</DropdownMenuLabel>
        <DropdownMenuCheckboxItem
          checked={showHidden}
          onCheckedChange={(v) => onShowHiddenChange(Boolean(v))}
          onSelect={(e) => e.preventDefault()}
        >
          <div className="flex flex-col">
            <span>Show hidden files</span>
            <span className="text-xs text-muted-foreground">
              Files starting with a dot
            </span>
          </div>
        </DropdownMenuCheckboxItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
