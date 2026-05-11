import { readCSRFToken } from "@/lib/csrf";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

type Json = Record<string, unknown> | unknown[] | null;

interface ApiOptions extends Omit<RequestInit, "body"> {
  json?: Json;
  body?: BodyInit | null;
}

/**
 * api is the single typed entry point for backend calls. It:
 *   - sets credentials: 'include' so cookies flow,
 *   - attaches X-CSRF-Token from the storman_csrf cookie on mutating verbs,
 *   - JSON-encodes options.json automatically,
 *   - throws ApiError on non-2xx responses (with the server's error string when present),
 *   - returns undefined for 204, parsed JSON otherwise.
 *
 * Use rawFetch() when you need access to the response object (Range requests,
 * progress, blob downloads).
 */
export async function api<T>(path: string, opts: ApiOptions = {}): Promise<T> {
  const res = await rawFetch(path, opts);
  if (res.status === 204) return undefined as T;
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) {
    return (await res.json()) as T;
  }
  return (await res.text()) as unknown as T;
}

export async function rawFetch(path: string, opts: ApiOptions = {}): Promise<Response> {
  const headers = new Headers(opts.headers);
  const method = (opts.method ?? "GET").toUpperCase();
  let body = opts.body ?? null;

  if (opts.json !== undefined) {
    body = JSON.stringify(opts.json);
    if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  }

  if (method !== "GET" && method !== "HEAD") {
    const csrf = readCSRFToken();
    if (csrf) headers.set("X-CSRF-Token", csrf);
  }

  const res = await fetch(path, {
    ...opts,
    method,
    headers,
    body,
    credentials: "include",
  });

  if (!res.ok) {
    let message = res.statusText;
    try {
      const data = (await res.clone().json()) as { error?: string };
      if (data && typeof data.error === "string") message = data.error;
    } catch {
      // ignore — fall back to statusText
    }
    throw new ApiError(res.status, message);
  }
  return res;
}
