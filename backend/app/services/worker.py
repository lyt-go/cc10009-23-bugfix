"""采集节点业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services.bandwidth import bandwidth_service
from app.store import store

MODULE = "worker"
REQUIRED_FIELDS = ["节点编号", "主机地址", "并发槽位数"]
STATUS_ORDER = ["草稿", "注册中", "在线", "离线"]
ACTION_RULES = {"提交注册": "注册中", "标记在线": "在线", "标记离线": "离线"}
NEGATIVE_ACTIONS = []


class WorkerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        start = max(page - 1, 0) * size
        page_rows = rows[start:start + size]
        # 集群节点侧清单读取前先对齐带宽分配，避免旧配额残留在列表里。
        bandwidth_service.sync_manifests(page_rows)
        return page_rows, len(rows)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is not None:
            bandwidth_service.sync_manifests([entry])
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        node = str(values.get("节点编号")).strip()
        if any(str(row.get("节点编号") or "").strip() == node for row in rows):
            return None, [f"节点编号 {node} 已存在"]
        entry = {"id": store.next_id(rows)}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"工作节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集节点可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        node = str(entry.get("节点编号") or "").strip()
        if action == "标记离线":
            # 节点离线保留当时的带宽执行记录，不删除配额。
            bandwidth_service.handle_node_offline(node)
            entry["status"] = target
            entry["pending"] = False
            entry["abnormal"] = False
            return entry, "工作节点已标记离线；当时生效的带宽执行记录已留档"
        if action == "标记在线":
            entry["status"] = target
            entry["pending"] = False
            entry["abnormal"] = False
            bandwidth_service.handle_node_online(node)
            return entry, "工作节点已标记在线；带宽配额按最新有效策略对齐"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"工作节点已{action}"
