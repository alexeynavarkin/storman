import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { useForm } from "react-hook-form";
import { Navigate, useNavigate } from "react-router";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { setupStateQueryKey, useSetupState } from "@/features/setup/use-setup-state";
import { ApiError, api } from "@/lib/api";

const setupSchema = z
  .object({
    token: z.string().min(1, "Setup token is required"),
    login: z.string().min(1, "Login is required"),
    password: z.string().min(12, "Use at least 12 characters"),
    confirm: z.string().min(1, "Repeat the password"),
  })
  .refine((v) => v.password === v.confirm, {
    path: ["confirm"],
    message: "Passwords do not match",
  });

type SetupValues = z.infer<typeof setupSchema>;

export function SetupRoute() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const state = useSetupState();

  const form = useForm<SetupValues>({
    resolver: zodResolver(setupSchema),
    defaultValues: { token: "", login: "", password: "", confirm: "" },
    mode: "onBlur",
  });

  const mutation = useMutation({
    mutationFn: (values: SetupValues) =>
      api<{ login: string }>("/api/setup/admin", {
        method: "POST",
        json: { token: values.token, login: values.login, password: values.password },
      }),
    onSuccess: () => {
      queryClient.setQueryData(setupStateQueryKey, { needs_setup: false });
      navigate("/login", { replace: true });
    },
    onError: (err) => {
      const message = err instanceof ApiError ? err.message : "Setup failed";
      if (err instanceof ApiError && err.status === 401) {
        form.setError("token", { message: "Invalid setup token" });
      } else {
        form.setError("password", { message });
      }
    },
  });

  if (state.isLoading) return <div className="min-h-svh bg-background" />;
  if (state.data && !state.data.needs_setup) return <Navigate to="/login" replace />;

  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-4">
      <div className="w-full max-w-md">
        <div className="mb-8 flex flex-col items-start gap-1">
          <span className="text-xs font-medium tracking-wider uppercase text-muted-foreground">
            storman · first-run setup
          </span>
          <h1 className="text-2xl font-semibold tracking-tight">Create administrator</h1>
          <p className="text-sm text-muted-foreground">
            Paste the one-time setup token from the server logs and pick a login
            and password for the first administrator account.
          </p>
        </div>

        <form
          onSubmit={form.handleSubmit((v) => mutation.mutate(v))}
          className="flex flex-col gap-4"
          noValidate
        >
          <Field
            id="token"
            label="Setup token"
            error={form.formState.errors.token?.message}
            input={
              <Input
                id="token"
                autoComplete="off"
                autoFocus
                aria-invalid={!!form.formState.errors.token}
                {...form.register("token")}
              />
            }
          />
          <Field
            id="login"
            label="Login"
            error={form.formState.errors.login?.message}
            input={
              <Input
                id="login"
                autoComplete="username"
                aria-invalid={!!form.formState.errors.login}
                {...form.register("login")}
              />
            }
          />
          <Field
            id="password"
            label="Password"
            error={form.formState.errors.password?.message}
            input={
              <Input
                id="password"
                type="password"
                autoComplete="new-password"
                aria-invalid={!!form.formState.errors.password}
                {...form.register("password")}
              />
            }
          />
          <Field
            id="confirm"
            label="Repeat password"
            error={form.formState.errors.confirm?.message}
            input={
              <Input
                id="confirm"
                type="password"
                autoComplete="new-password"
                aria-invalid={!!form.formState.errors.confirm}
                {...form.register("confirm")}
              />
            }
          />

          <Button type="submit" disabled={mutation.isPending} className="mt-2 w-full">
            {mutation.isPending ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              "Create administrator"
            )}
          </Button>
        </form>
      </div>
    </main>
  );
}

function Field({
  id,
  label,
  error,
  input,
}: {
  id: string;
  label: string;
  error?: string;
  input: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor={id}>{label}</Label>
      {input}
      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  );
}
