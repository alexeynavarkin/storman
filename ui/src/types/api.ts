export interface UserDTO {
  id: string;
  login: string;
  created_at?: string;
}

export interface MeResponse {
  id: string;
  login: string;
  is_root_admin: boolean;
}

export interface NodeInfo {
  id: string;
  name: string;
  path: string;
  type: "file" | "dir";
  size: number;
  mime?: string;
  mtime?: string;
  sha256?: string;
  children?: number;
  effective: number;
}

export interface PermEntry {
  user_id: string;
  login: string;
  actions: number;
}

export interface PermListResponse {
  node_id: string;
  effective_mine: number;
  explicit: PermEntry[];
}

export interface AuditRow {
  id: number;
  ts: string;
  user_id?: string;
  action: string;
  node_id?: string;
  ip?: string;
  result: "ok" | "denied" | "error";
  details?: Record<string, unknown>;
}

export interface TrashEntry {
  trash_uuid: string;
  root_node_id: string;
  root_logical_path: string;
  root_node_type: "file" | "dir";
  deleted_by?: string;
  deleted_at: string;
  size_bytes: number;
  can_restore: boolean;
  can_purge: boolean;
}
