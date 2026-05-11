import { useQuery } from "@tanstack/react-query";

import { ApiError, api } from "@/lib/api";
import type { MeResponse } from "@/types/api";

export const currentUserQueryKey = ["me"] as const;

export function useCurrentUser() {
  return useQuery<MeResponse | null>({
    queryKey: currentUserQueryKey,
    queryFn: async () => {
      try {
        return await api<MeResponse>("/api/auth/me");
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) return null;
        throw err;
      }
    },
    staleTime: 60_000,
  });
}
