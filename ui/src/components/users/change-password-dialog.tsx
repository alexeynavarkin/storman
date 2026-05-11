import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { toast } from "sonner";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { currentUserQueryKey } from "@/features/auth/use-current-user";
import { ApiError, api } from "@/lib/api";

const schema = z
  .object({
    password: z.string().min(12, "At least 12 characters"),
    confirm: z.string(),
  })
  .refine((v) => v.password === v.confirm, {
    message: "Passwords do not match",
    path: ["confirm"],
  });

interface ChangePasswordDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** UUID of the user whose password is being set. */
  userId: string;
  /** Display name in the dialog body. */
  login: string;
  /** True when the caller is changing their own password. The backend
   *  preserves the current session in this case; we still re-fetch /me. */
  isSelf: boolean;
}

export function ChangePasswordDialog({
  open,
  onOpenChange,
  userId,
  login,
  isSelf,
}: ChangePasswordDialogProps) {
  const queryClient = useQueryClient();
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: { password: "", confirm: "" },
  });

  useEffect(() => {
    if (open) form.reset({ password: "", confirm: "" });
  }, [open, form]);

  const mutation = useMutation({
    mutationFn: (values: z.infer<typeof schema>) =>
      api<void>(`/api/users/${userId}/password`, {
        method: "POST",
        json: { password: values.password },
      }),
    onSuccess: () => {
      if (isSelf) {
        queryClient.invalidateQueries({ queryKey: currentUserQueryKey });
      }
      toast.success("Password updated");
      onOpenChange(false);
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : "Failed to update password";
      form.setError("password", { message: msg });
    },
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{isSelf ? "Change your password" : `Reset password for ${login}`}</DialogTitle>
          <DialogDescription>
            {isSelf
              ? "Other devices will be signed out. This session keeps working."
              : `All of ${login}'s active sessions will be terminated.`}
          </DialogDescription>
        </DialogHeader>
        <form
          onSubmit={form.handleSubmit((v) => mutation.mutate(v))}
          className="flex flex-col gap-3"
          noValidate
        >
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="pw-new">New password</Label>
            <Input
              id="pw-new"
              type="password"
              autoFocus
              autoComplete="new-password"
              aria-invalid={!!form.formState.errors.password}
              {...form.register("password")}
            />
            {form.formState.errors.password && (
              <p className="text-xs text-destructive">
                {form.formState.errors.password.message}
              </p>
            )}
          </div>

          <div className="flex flex-col gap-1.5">
            <Label htmlFor="pw-confirm">Confirm</Label>
            <Input
              id="pw-confirm"
              type="password"
              autoComplete="new-password"
              aria-invalid={!!form.formState.errors.confirm}
              {...form.register("confirm")}
            />
            {form.formState.errors.confirm && (
              <p className="text-xs text-destructive">
                {form.formState.errors.confirm.message}
              </p>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="secondary"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={mutation.isPending}>
              {mutation.isPending ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                "Save"
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
