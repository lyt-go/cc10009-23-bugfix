"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

除了按业务模块维护的 ``_tables``，这里还保存带宽控制派生出来的三张数据集：

- ``bandwidth_allocation``：节点带宽分配（同一个节点可以有多个历史版本，但只有一条生效）；
- ``bandwidth_execution``：限速执行结果（同一个节点只保留一条，切换策略时原地更新）；
- ``bandwidth_execution_history``：限速执行的切换/离线留档，用来追溯“当时执行的是什么”。
- ``bandwidth_traffic``：下载任务回写的实际流量流水。

派生数据集不参与运营概览的“业务模块”统计，避免把内部结果集当成业务模块展示。
"""
from __future__ import annotations

import copy
from typing import Any

from app.seed import SEED_ROWS

DERIVED_TABLES = (
    "bandwidth_allocation",
    "bandwidth_execution",
    "bandwidth_execution_history",
    "bandwidth_traffic",
)


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._derived: dict[str, list[dict[str, Any]]] = {
            name: [] for name in DERIVED_TABLES
        }

    def reset(self) -> None:
        """把仓库恢复到示例数据初始状态，测试和本地调试都用得上。"""
        self._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._derived = {name: [] for name in DERIVED_TABLES}

    def snapshot(self) -> dict[str, Any]:
        """深拷贝当前全部数据，供策略切换失败时整体回滚。"""
        return {
            "tables": copy.deepcopy(self._tables),
            "derived": copy.deepcopy(self._derived),
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        self._tables = copy.deepcopy(snapshot["tables"])
        self._derived = copy.deepcopy(snapshot["derived"])

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def dataset_names(self) -> list[str]:
        return sorted(self._derived)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def derived(self, name: str) -> list[dict[str, Any]]:
        return self._derived.setdefault(name, [])

    def allocations(self) -> list[dict[str, Any]]:
        return self.derived("bandwidth_allocation")

    def executions(self) -> list[dict[str, Any]]:
        return self.derived("bandwidth_execution")

    def execution_history(self) -> list[dict[str, Any]]:
        return self.derived("bandwidth_execution_history")

    def traffic(self) -> list[dict[str, Any]]:
        return self.derived("bandwidth_traffic")

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
