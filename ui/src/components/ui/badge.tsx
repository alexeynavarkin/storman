import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";

import { cn } from "@/lib/cn";

const badgeVariants = cva(
  "inline-flex items-center rounded px-1.5 py-0.5 text-xs font-medium tracking-wide",
  {
    variants: {
      variant: {
        default: "bg-surface-elevated text-foreground border border-border",
        muted: "bg-transparent text-muted-foreground border border-border",
        accent: "bg-primary/10 text-foreground border border-primary/20",
        destructive: "bg-destructive/10 text-destructive border border-destructive/20",
      },
    },
    defaultVariants: { variant: "default" },
  },
);

interface BadgeProps
  extends ComponentProps<"span">,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />;
}
