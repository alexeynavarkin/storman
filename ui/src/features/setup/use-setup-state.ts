import { useQuery } from "@tanstack/react-query";

import { api } from "@/lib/api";

export const setupStateQueryKey = ["setup-state"] as const;

export interface SetupState {
  needs_setup: boolean;
}

/**
 * useSetupState polls /api/setup/state once on mount. Setup state only changes
 * across server restarts (or when the first admin is created in this session),
 * so we keep the result fresh for the entire session — the createAdmin mutation
 * invalidates the cache on success to flip needs_setup to false instantly.
 */
export function useSetupState() {
  return useQuery<SetupState>({
    queryKey: setupStateQueryKey,
    queryFn: () => api<SetupState>("/api/setup/state"),
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
}
