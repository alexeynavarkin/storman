// Mirror of internal/rbac/actions.go. Keep bit layout in sync — backend is
// authoritative; this enum is for affordances (enabling/disabling UI controls).

export const Action = {
  Read: 1 << 0,
  Write: 1 << 1,
  Remove: 1 << 2,
  Admin: 1 << 3,
  Traverse: 1 << 4,
} as const;

export type ActionKey = keyof typeof Action;
export type ActionBit = (typeof Action)[ActionKey];

/** Storable bits (not Traverse) — used by Grant/Revoke. */
export const STORABLE_ACTIONS: readonly ActionBit[] = [
  Action.Read,
  Action.Write,
  Action.Remove,
  Action.Admin,
];

export function hasAction(mask: number, action: ActionBit): boolean {
  return (mask & action) === action;
}

export function setAction(mask: number, action: ActionBit, on: boolean): number {
  return on ? mask | action : mask & ~action;
}

export function labelFor(action: ActionBit): string {
  switch (action) {
    case Action.Read: return "Read";
    case Action.Write: return "Write";
    case Action.Remove: return "Remove";
    case Action.Admin: return "Admin";
    case Action.Traverse: return "Traverse";
    default: return "?";
  }
}
