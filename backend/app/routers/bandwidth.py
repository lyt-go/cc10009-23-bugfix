"""带宽控制接口。

除了策略本身的增查改，这里还暴露带宽策略派生出来的数据集：

- ``/allocations``：节点带宽分配（多版本，可筛选当前生效版本）；
- ``/executions``：限速执行结果（同一节点只有一条）；
- ``/history``：执行结果留档，用于追溯切换/离线时实际执行的策略快照；
- ``/traffic``：下载任务回写的实际流量；
- ``/nodes/{node}``：集群节点侧读取分配与执行结果；
- ``/nodes/{node}/traffic``：下载任务按配额执行后回写实际流量。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.bandwidth import BandwidthError, bandwidth_service

router = APIRouter(prefix="/api/bandwidth", tags=["带宽控制"])

service = bandwidth_service

LIST_FIELDS = ["策略编号", "节点标识", "下载带宽上限", "上传带宽上限", "突发额度", "限速粒度秒", "优先级权重", "生效时段"]
STATUSES = ["草稿", "策略评审中", "已启用", "已暂停"]


def _page(items: list[dict[str, Any]], total: int, page: int, size: int) -> PageResult[dict]:
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、策略评审中、已启用、已暂停"),
    node: str | None = Query(default=None, description="按节点标识检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号、状态与节点标识过滤带宽控制列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(
        keyword=keyword, status=status, node=node, page=page, size=size
    )
    return _page(items, total, page, size)


# -- 派生数据集（放在 /{entry_id} 之前，避免路径被整数解析吃掉） -------------


@router.get("/allocations", response_model=PageResult[dict])
def list_allocations(
    node: str | None = Query(default=None, description="按节点标识过滤"),
    status: str | None = Query(default=None),
    policy_id: str | None = Query(default=None, description="按策略编号过滤"),
    only_current: bool = Query(default=False, description="只看当前生效/沿用版本"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """节点带宽分配数据集：同一节点会有多个历史版本，用于解释冲突与空窗。"""
    items, total = service.list_allocations(
        node=node,
        status=status,
        policy_id=policy_id,
        only_current=only_current,
        page=page,
        size=size,
    )
    return _page(items, total, page, size)


@router.get("/executions", response_model=PageResult[dict])
def list_executions(
    node: str | None = Query(default=None),
    node_status: str | None = Query(default=None, description="在线、离线"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """限速执行结果数据集：每个节点最多一条。"""
    items, total = service.list_executions(
        node=node, node_status=node_status, page=page, size=size
    )
    return _page(items, total, page, size)


@router.get("/history", response_model=PageResult[dict])
def list_history(
    node: str | None = Query(default=None),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """限速执行留档：记录切换、沿用、离线时实际生效的策略快照。"""
    items, total = service.list_history(node=node, page=page, size=size)
    return _page(items, total, page, size)


@router.get("/traffic", response_model=PageResult[dict])
def list_traffic(
    node: str | None = Query(default=None),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """下载任务实际流量回写流水。"""
    items, total = service.list_traffic(node=node, page=page, size=size)
    return _page(items, total, page, size)


@router.post("/reconcile", response_model=ActionResult)
def reconcile(payload: EntryPayload) -> ActionResult:
    """按指定时间校准生效时段：到期的降级，空窗沿用上一有效配额。"""
    values = payload.values
    node = str(values.get("节点标识") or values.get("node") or "").strip()
    try:
        if node:
            execution = service.reconcile_node(node, values.get("at"))
            if execution is None:
                return ActionResult(ok=False, message=f"节点 {node} 没有可校准的带宽分配")
            return ActionResult(ok=True, message=f"节点 {node} 带宽配额已按生效时段校准", entry=execution)
        results = service.reconcile_all(values.get("at"))
    except BandwidthError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(
        ok=True,
        message=f"已校准 {len(results)} 个节点的带宽配额",
        entry={"节点数": len(results), "执行结果": results},
    )


@router.post("/batch", response_model=ActionResult)
def submit_batch(payload: EntryPayload) -> ActionResult:
    """同一节点的多条带宽记录一次提交整组，逐条返回唯一配额结果。"""
    entry, message = service.submit_allocations(payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/nodes/{node}", response_model=dict)
def get_node_bandwidth(
    node: str,
    at: str | None = Query(default=None, description="按指定时间读取，格式 YYYY-MM-DDTHH:MM:SS"),
) -> dict:
    """集群节点侧模块读取节点带宽分配与唯一限速执行结果。"""
    try:
        bandwidth = service.node_bandwidth(node, at)
    except BandwidthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if bandwidth is None:
        raise HTTPException(status_code=404, detail=f"节点 {node} 尚未分配带宽")
    return bandwidth


@router.post("/nodes/{node}/traffic", response_model=ActionResult)
def report_traffic(node: str, payload: EntryPayload) -> ActionResult:
    """下载任务按节点配额执行，并把实际流量回写到执行结果。"""
    entry, message = service.report_traffic(node, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=not entry.get("是否超限", False), message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出带宽控制清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "bandwidth", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条带宽策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"带宽策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条带宽策略，缺字段时说明原因而不是静默丢弃。"""
    entry, errors = service.create_entry(payload.values)
    if entry is None:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="带宽策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行提交策略评审、启用策略、暂停策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
