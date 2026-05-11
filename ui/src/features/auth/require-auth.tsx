import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router";

import { Skeleton } from "@/components/ui/skeleton";
import { useCurrentUser } from "@/features/auth/use-current-user";

export function RequireAuth({ children }: { children: ReactNode }) {
  const me = useCurrentUser();
  const location = useLocation();

  if (me.isLoading) {
    return (
      <div className="flex h-svh items-center justify-center">
        <Skeleton className="h-10 w-48" />
      </div>
    );
  }
  if (!me.data) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return <>{children}</>;
}
