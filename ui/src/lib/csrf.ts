// readCSRFToken reads the storman_csrf cookie. The backend sets it on login
// and we must echo it back via X-CSRF-Token on every mutating request.
export function readCSRFToken(): string | null {
  const cookies = document.cookie.split(";");
  for (const c of cookies) {
    const idx = c.indexOf("=");
    if (idx < 0) continue;
    const name = c.slice(0, idx).trim();
    if (name === "storman_csrf") {
      return decodeURIComponent(c.slice(idx + 1));
    }
  }
  return null;
}
