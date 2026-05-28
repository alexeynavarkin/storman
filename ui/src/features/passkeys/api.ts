import type {
  PublicKeyCredentialCreationOptionsJSON,
  PublicKeyCredentialRequestOptionsJSON,
} from "@simplewebauthn/browser";

import { api } from "@/lib/api";
import type { MeResponse, PasskeyDTO } from "@/types/api";

interface BeginRegistrationResponse {
  publicKey: PublicKeyCredentialCreationOptionsJSON;
  challenge_id: string;
}

interface BeginLoginResponse {
  publicKey: PublicKeyCredentialRequestOptionsJSON;
  challenge_id: string;
}

export const passkeysQueryKey = ["passkeys"] as const;

export function listPasskeys() {
  return api<{ passkeys: PasskeyDTO[] }>("/api/passkeys").then((r) => r.passkeys);
}

export function beginRegister() {
  return api<BeginRegistrationResponse>("/api/passkeys/register/begin", { method: "POST" });
}

export function finishRegister(challengeId: string, name: string, credential: unknown) {
  return api<PasskeyDTO>("/api/passkeys/register/finish", {
    method: "POST",
    json: { challenge_id: challengeId, name, credential },
  });
}

export function renamePasskey(id: string, name: string) {
  return api<void>(`/api/passkeys/${id}`, { method: "PATCH", json: { name } });
}

export function deletePasskey(id: string) {
  return api<void>(`/api/passkeys/${id}`, { method: "DELETE" });
}

export function beginLogin() {
  return api<BeginLoginResponse>("/api/auth/passkey/login/begin", { method: "POST" });
}

export function finishLogin(challengeId: string, assertion: unknown) {
  return api<MeResponse>("/api/auth/passkey/login/finish", {
    method: "POST",
    json: { challenge_id: challengeId, assertion },
  });
}
