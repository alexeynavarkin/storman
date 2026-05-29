import { lazy, Suspense, type ReactNode } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router";

import { RequireAuth } from "@/features/auth/require-auth";
import { useSetupState } from "@/features/setup/use-setup-state";
import { LoginRoute } from "@/routes/login";
import { SetupRoute } from "@/routes/setup";

// Files is loaded eagerly — it's the default destination after login, so the
// initial paint already needs it. Users and Trash are admin-only and rarely
// the entry point, so they ship as separate chunks.
import { FilesRoute } from "@/routes/files";

const UsersRoute = lazy(() => import("@/routes/users").then((m) => ({ default: m.UsersRoute })));
const TrashRoute = lazy(() => import("@/routes/trash").then((m) => ({ default: m.TrashRoute })));
const AuditRoute = lazy(() => import("@/routes/audit").then((m) => ({ default: m.AuditRoute })));
const SettingsRoute = lazy(() => import("@/routes/settings").then((m) => ({ default: m.SettingsRoute })));

export function RootLayout() {
  const setup = useSetupState();
  const location = useLocation();

  // While probing setup state, render a blank canvas. The check is one cheap
  // HTTP call so the wait is imperceptible after the first paint.
  if (setup.isLoading) return <div className="min-h-svh bg-background" />;

  // Setup not yet done → force the wizard. Allow /setup itself so the form
  // can mount; everything else redirects.
  if (setup.data?.needs_setup && location.pathname !== "/setup") {
    return <Navigate to="/setup" replace />;
  }

  return (
    <Routes>
      <Route path="/setup" element={<SetupRoute />} />
      <Route path="/login" element={<LoginRoute />} />
      <Route
        path="/files/*"
        element={
          <RequireAuth>
            <FilesRoute />
          </RequireAuth>
        }
      />
      <Route
        path="/users"
        element={
          <RequireAuth>
            <Lazy>
              <UsersRoute />
            </Lazy>
          </RequireAuth>
        }
      />
      <Route
        path="/trash"
        element={
          <RequireAuth>
            <Lazy>
              <TrashRoute />
            </Lazy>
          </RequireAuth>
        }
      />
      <Route
        path="/audit"
        element={
          <RequireAuth>
            <Lazy>
              <AuditRoute />
            </Lazy>
          </RequireAuth>
        }
      />
      <Route
        path="/settings"
        element={
          <RequireAuth>
            <Lazy>
              <SettingsRoute />
            </Lazy>
          </RequireAuth>
        }
      />
      <Route path="/" element={<Navigate to="/files/" replace />} />
      <Route path="*" element={<Navigate to="/files/" replace />} />
    </Routes>
  );
}

// Lazy wraps lazy() routes with the project's standard fallback so the SPA
// shell stays visible during the chunk fetch. The fallback is minimal — a
// blank canvas of the right colour is less jarring than a flash of unstyled
// content.
function Lazy({ children }: { children: ReactNode }) {
  return <Suspense fallback={<div className="min-h-svh bg-background" />}>{children}</Suspense>;
}
