import { lazy, Suspense, type ReactNode } from "react";
import { Navigate, Route, Routes } from "react-router";

import { RequireAuth } from "@/features/auth/require-auth";
import { LoginRoute } from "@/routes/login";

// Files is loaded eagerly — it's the default destination after login, so the
// initial paint already needs it. Users and Trash are admin-only and rarely
// the entry point, so they ship as separate chunks.
import { FilesRoute } from "@/routes/files";

const UsersRoute = lazy(() => import("@/routes/users").then((m) => ({ default: m.UsersRoute })));
const TrashRoute = lazy(() => import("@/routes/trash").then((m) => ({ default: m.TrashRoute })));
const AuditRoute = lazy(() => import("@/routes/audit").then((m) => ({ default: m.AuditRoute })));

export function RootLayout() {
  return (
    <Routes>
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
