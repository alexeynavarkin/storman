import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { startRegistration } from "@simplewebauthn/browser";
import { Loader2, Trash2 } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

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
import {
  beginRegister,
  deletePasskey,
  finishRegister,
  listPasskeys,
  passkeysQueryKey,
} from "@/features/passkeys/api";
import { ApiError } from "@/lib/api";

interface PasskeysDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function PasskeysDialog({ open, onOpenChange }: PasskeysDialogProps) {
  const queryClient = useQueryClient();
  const supported = typeof window !== "undefined" && !!window.PublicKeyCredential;
  const [newName, setNewName] = useState("");

  const passkeys = useQuery({
    queryKey: passkeysQueryKey,
    queryFn: listPasskeys,
    enabled: open,
  });

  const register = useMutation({
    mutationFn: async (name: string) => {
      const begin = await beginRegister();
      let credential;
      try {
        credential = await startRegistration({ optionsJSON: begin.publicKey });
      } catch (err) {
        // NotAllowedError = user cancelled or timed out; surface a soft message.
        if (err instanceof Error && err.name === "NotAllowedError") {
          throw new Error("Registration cancelled");
        }
        throw err;
      }
      return finishRegister(begin.challenge_id, name, credential);
    },
    onSuccess: () => {
      setNewName("");
      queryClient.invalidateQueries({ queryKey: passkeysQueryKey });
      toast.success("Passkey added");
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : err instanceof Error ? err.message : "Failed to add passkey";
      toast.error(msg);
    },
  });

  const remove = useMutation({
    mutationFn: (id: string) => deletePasskey(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: passkeysQueryKey });
      toast.success("Passkey removed");
    },
    onError: (err) => {
      const msg = err instanceof ApiError ? err.message : "Failed to remove passkey";
      toast.error(msg);
    },
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Passkeys</DialogTitle>
          <DialogDescription>
            Sign in to Storman without a password using a hardware key, Touch ID, or
            Windows Hello. Your password keeps working as a fallback.
          </DialogDescription>
        </DialogHeader>

        {!supported && (
          <p className="text-xs text-destructive">
            This browser does not support WebAuthn.
          </p>
        )}

        <div className="flex flex-col gap-2">
          {passkeys.isLoading && <p className="text-xs text-muted-foreground">Loading…</p>}
          {passkeys.data?.length === 0 && (
            <p className="text-xs text-muted-foreground">No passkeys registered yet.</p>
          )}
          {passkeys.data?.map((p) => (
            <div
              key={p.id}
              className="flex items-center justify-between rounded-md border border-border px-3 py-2"
            >
              <div className="flex flex-col">
                <span className="text-sm font-medium">{p.name}</span>
                <span className="text-xs text-muted-foreground">
                  Added {new Date(p.created_at).toLocaleDateString()}
                  {p.last_used_at && ` · last used ${new Date(p.last_used_at).toLocaleDateString()}`}
                </span>
              </div>
              <Button
                variant="ghost"
                size="sm"
                aria-label="Remove passkey"
                disabled={remove.isPending}
                onClick={() => remove.mutate(p.id)}
              >
                <Trash2 className="size-4" />
              </Button>
            </div>
          ))}
        </div>

        <div className="flex flex-col gap-2 border-t border-border pt-3">
          <Label htmlFor="pk-name">Add a new passkey</Label>
          <div className="flex gap-2">
            <Input
              id="pk-name"
              placeholder="e.g. MacBook Touch ID"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              disabled={!supported || register.isPending}
            />
            <Button
              type="button"
              disabled={!supported || !newName.trim() || register.isPending}
              onClick={() => register.mutate(newName.trim())}
            >
              {register.isPending ? <Loader2 className="size-4 animate-spin" /> : "Add"}
            </Button>
          </div>
        </div>

        <DialogFooter>
          <Button variant="secondary" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
