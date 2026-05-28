import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { startAuthentication } from "@simplewebauthn/browser";
import { Fingerprint, Loader2 } from "lucide-react";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { useLocation, useNavigate } from "react-router";
import { toast } from "sonner";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { currentUserQueryKey, useCurrentUser } from "@/features/auth/use-current-user";
import { beginLogin, finishLogin } from "@/features/passkeys/api";
import { ApiError, api } from "@/lib/api";
import type { MeResponse } from "@/types/api";

const loginSchema = z.object({
  login: z.string().min(1, "Login is required"),
  password: z.string().min(1, "Password is required"),
});

type LoginValues = z.infer<typeof loginSchema>;

interface LocationState {
  from?: string;
}

export function LoginRoute() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const me = useCurrentUser();

  const from = (location.state as LocationState | null)?.from ?? "/files/";

  // Already logged in? Skip the form.
  useEffect(() => {
    if (me.data) navigate(from, { replace: true });
  }, [me.data, from, navigate]);

  const form = useForm<LoginValues>({
    resolver: zodResolver(loginSchema),
    defaultValues: { login: "", password: "" },
    mode: "onBlur",
  });

  const mutation = useMutation({
    mutationFn: (values: LoginValues) =>
      api<MeResponse>("/api/auth/login", { method: "POST", json: values }),
    onSuccess: (user) => {
      queryClient.setQueryData(currentUserQueryKey, user);
      navigate(from, { replace: true });
    },
    onError: (err) => {
      const message = err instanceof ApiError ? err.message : "Login failed";
      form.setError("password", { message });
    },
  });

  const passkey = useMutation({
    mutationFn: async () => {
      const begin = await beginLogin();
      const assertion = await startAuthentication({ optionsJSON: begin.publicKey });
      return finishLogin(begin.challenge_id, assertion);
    },
    onSuccess: (user) => {
      queryClient.setQueryData(currentUserQueryKey, user);
      navigate(from, { replace: true });
    },
    onError: (err) => {
      // NotAllowedError = user cancelled. Stay silent in that case.
      if (err instanceof Error && err.name === "NotAllowedError") return;
      const message = err instanceof ApiError ? err.message : err instanceof Error ? err.message : "Passkey sign-in failed";
      toast.error(message);
    },
  });

  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-4">
      <div className="w-full max-w-sm">
        <div className="mb-8 flex flex-col items-start gap-1">
          <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
            storman
          </span>
          <h1 className="text-2xl font-semibold tracking-tight">Sign in</h1>
          <p className="text-sm text-muted-foreground">
            Use your operator credentials to access the file store.
          </p>
        </div>

        <form
          onSubmit={form.handleSubmit((v) => mutation.mutate(v))}
          className="flex flex-col gap-4"
          noValidate
        >
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="login">Login</Label>
            <Input
              id="login"
              autoComplete="username"
              autoFocus
              aria-invalid={!!form.formState.errors.login}
              {...form.register("login")}
            />
            {form.formState.errors.login && (
              <p className="text-xs text-destructive">
                {form.formState.errors.login.message}
              </p>
            )}
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              autoComplete="current-password"
              aria-invalid={!!form.formState.errors.password}
              {...form.register("password")}
            />
            {form.formState.errors.password && (
              <p className="text-xs text-destructive">
                {form.formState.errors.password.message}
              </p>
            )}
          </div>

          <Button type="submit" disabled={mutation.isPending} className="mt-2 w-full">
            {mutation.isPending ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              "Sign in"
            )}
          </Button>

          <Button
            type="button"
            variant="secondary"
            disabled={passkey.isPending}
            onClick={() => passkey.mutate()}
            className="w-full gap-2"
          >
            {passkey.isPending ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              <>
                <Fingerprint className="size-4" />
                Sign in with passkey
              </>
            )}
          </Button>
        </form>
      </div>
    </main>
  );
}
