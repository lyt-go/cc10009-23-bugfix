"""带宽控制业务规则。

这一层负责带宽策略的完整生命周期，以及由策略派生出来的几份数据集：

- 节点带宽分配（``bandwidth_allocation``）：按节点、按策略版本留档；同一节点可以有多条
  历史版本，但任意时刻只有一条处于「生效」或「沿用」状态。
- 限速执行结果（``bandwidth_execution``）：同一节点只允许一条记录，策略切换时原地更新，
  旧值写入 ``bandwidth_execution_history``，保证切换/离线后仍能追溯当时执行的配额。
- 下载流量回写（``bandwidth_traffic``）：下载任务按分配到的配额执行，并把实际流量写回。

几个关键约束（对应线上事故）：

1. 策略必须走完评审才能启用，暂停后恢复同样要重新评审；
2. 同一节点的多条策略冲突时按优先级、策略版本、登记顺序选出唯一赢家，绝不叠加额度；
3. 重复启用/重复提交按幂等处理，不会重复生成分配或执行结果；
4. 策略切换失败时先在副本上计算，失败即回滚，旧配额与旧执行结果继续生效；
5. 生效时段出现空窗时沿用上一有效配额，而不是让节点短暂没有合法分配；
6. 节点离线只标记执行结果状态并留档，不删除当时的配额记录。
"""
from __future__ import annotations

import copy
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any

from app.store import store

MODULE = "bandwidth"
REQUIRED_FIELDS = ["策略编号", "节点标识", "下载带宽上限"]
STATUS_ORDER = ["草稿", "策略评审中", "已启用", "已暂停"]

# 动作 -> (允许的来源状态, 目标状态)
ACTION_RULES: dict[str, tuple[tuple[str, ...], str]] = {
    "提交策略评审": (("草稿", "已暂停", "策略评审中"), "策略评审中"),
    "启用策略": (("策略评审中", "已启用"), "已启用"),
    "暂停策略": (("已启用", "已暂停"), "已暂停"),
    # 暂停后的恢复统一走重新评审，避免旧策略绕过评审直接生效。
    "恢复启用": (("已暂停",), "策略评审中"),
    "重新评审": (("已暂停",), "策略评审中"),
}
NEGATIVE_ACTIONS = ["暂停策略"]

POLICY_FIELDS = [
    "策略编号",
    "节点标识",
    "下载带宽上限",
    "上传带宽上限",
    "突发额度",
    "限速粒度秒",
    "优先级权重",
    "生效时段",
    "策略版本",
]

ALLOCATION_ACTIVE = ("生效", "沿用")
TRUE_VALUES = {"1", "true", "yes", "y", "是", "开", "on"}


class BandwidthError(Exception):
    """带宽策略校验或切换失败；调用方据此保留旧配额。"""


# ---------------------------------------------------------------------------
# 解析工具
# ---------------------------------------------------------------------------

_RATE_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z/\.]*)\s*$")
_DAILY_PATTERN = re.compile(
    r"^\s*(\d{1,2}):(\d{2})\s*(?:-|~|－|—|–|至|到)\s*(\d{1,2}):(\d{2})\s*$"
)
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _number_text(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def parse_rate(value: Any, field_name: str, *, allow_zero: bool = False) -> dict[str, Any] | None:
    """把带宽文本归一化成 Mbps。

    支持 ``100Mbps``、``10MB/s``、``1Gbps``、``2000Kbps``、``100M``、纯数字（按 Mbps）。
    空值返回 ``None``；无法识别直接报错，避免把非法限速当成不限速放过去。
    ``allow_zero`` 用于突发额度这类可以为 0 的字段。
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = _number_text(float(value))
    else:
        raw = str(value).strip()
    if not raw:
        return None
    match = _RATE_PATTERN.match(raw)
    if not match:
        raise BandwidthError(f"{field_name}「{raw}」无法解析为带宽值")
    amount = float(match.group(1))
    unit_raw = match.group(2)
    unit = unit_raw.lower()
    if amount < 0:
        raise BandwidthError(f"{field_name}不能为负数")

    # 带大写 B 的写法按字节口径换算，其余按比特口径。
    byte_unit = "B" in unit_raw
    if byte_unit:
        factors = {
            "b/s": 8e-6,
            "kb/s": 8e-3,
            "kbyte/s": 8e-3,
            "kbytes/s": 8e-3,
            "mb/s": 8.0,
            "mbyte/s": 8.0,
            "mbytes/s": 8.0,
            "gb/s": 8000.0,
            "gbyte/s": 8000.0,
            "gbytes/s": 8000.0,
        }
        factor = factors.get(unit)
    else:
        factors = {
            "": 1.0,
            "m": 1.0,
            "mbps": 1.0,
            "mbit/s": 1.0,
            "mbit": 1.0,
            "m/s": 1.0,
            "g": 1000.0,
            "gbps": 1000.0,
            "gbit/s": 1000.0,
            "gbit": 1000.0,
            "g/s": 1000.0,
            "k": 0.001,
            "kbps": 0.001,
            "kbit/s": 0.001,
            "kbit": 0.001,
            "k/s": 0.001,
            "bps": 1e-6,
            "bit/s": 1e-6,
            "b/s": 1e-6,
        }
        factor = factors.get(unit)
    if factor is None:
        raise BandwidthError(f"{field_name}「{raw}」的带宽单位无法识别")
    mbps = amount * factor
    if mbps < 0 or (mbps == 0 and not allow_zero):
        raise BandwidthError(f"{field_name}必须大于 0")
    return {"原值": raw, "Mbps": round(mbps, 6), "显示": f"{_number_text(mbps)}Mbps"}


def _parse_moment(text: str, *, end_of_day: bool) -> datetime:
    text = text.strip()
    if not text:
        raise BandwidthError("生效时段存在空白端点")
    if _DATE_ONLY.match(text):
        day = date.fromisoformat(text)
        if end_of_day:
            return datetime.combine(day, time(23, 59, 59))
        return datetime.combine(day, time.min)
    normalized = text.replace("T", " ")
    for parser in (
        lambda value: datetime.fromisoformat(value),
        lambda value: datetime.strptime(value, "%Y-%m-%d %H:%M"),
        lambda value: datetime.strptime(value, "%Y/%m/%d %H:%M"),
        lambda value: datetime.strptime(value, "%Y/%m/%d"),
    ):
        try:
            return parser(normalized)
        except ValueError:
            continue
    raise BandwidthError(f"生效时段「{text}」无法解析")


@dataclass(frozen=True)
class EffectiveWindow:
    """生效时段：支持绝对区间与每天固定时段，空字符串表示长期有效。"""

    text: str
    always: bool = False
    start: datetime | None = None
    end: datetime | None = None
    daily_start: time | None = None
    daily_end: time | None = None

    def covers(self, moment: datetime) -> bool:
        if self.always:
            return True
        if self.daily_start is not None and self.daily_end is not None:
            clock = moment.time()
            if self.daily_start <= self.daily_end:
                return self.daily_start <= clock <= self.daily_end
            return clock >= self.daily_start or clock <= self.daily_end
        if self.start is not None and moment < self.start:
            return False
        if self.end is not None and moment > self.end:
            return False
        return True


def parse_window(value: Any) -> EffectiveWindow:
    """解析生效时段，支持 ``2026-01-01~2026-12-31``、``09:00-18:00`` 等写法。"""
    raw = "" if value is None else str(value).strip()
    if not raw or raw.lower() in {"长期", "全天", "不限", "always", "*", "all"}:
        return EffectiveWindow(text=raw or "长期", always=True)

    daily = _DAILY_PATTERN.match(raw)
    if daily:
        start_time = time(int(daily.group(1)), int(daily.group(2)))
        end_time = time(int(daily.group(3)), int(daily.group(4)))
        return EffectiveWindow(text=raw, daily_start=start_time, daily_end=end_time)

    normalized = raw
    for token in ("～", "至", "到", "—", "–", "－"):
        normalized = normalized.replace(token, "~")
    parts: list[str] = [item.strip() for item in normalized.split("~", 1)] if "~" in normalized else []
    if len(parts) == 2 and all(parts):
        start = _parse_moment(parts[0], end_of_day=False)
        end = _parse_moment(parts[1], end_of_day=True)
        if end < start:
            raise BandwidthError(f"生效时段「{raw}」的结束时间早于开始时间")
        return EffectiveWindow(text=raw, start=start, end=end)

    single = _parse_moment(raw, end_of_day=True)
    return EffectiveWindow(
        text=raw,
        start=single.replace(hour=0, minute=0, second=0, microsecond=0),
        end=single,
    )


def parse_priority(value: Any) -> float:
    if value is None or str(value).strip() == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    labels = {"最高": 20.0, "高": 10.0, "中": 5.0, "普通": 5.0, "低": 1.0}
    if text in labels:
        return labels[text]
    try:
        return float(text)
    except ValueError as exc:
        raise BandwidthError(f"优先级权重「{text}」无法解析") from exc


def parse_version(value: Any) -> int:
    if value is None or str(value).strip() == "":
        return 1
    text = str(value).strip().lower().lstrip("v")
    if not re.fullmatch(r"\d+", text):
        raise BandwidthError(f"策略版本「{value}」必须是正整数")
    version = int(text)
    if version < 1:
        raise BandwidthError("策略版本必须大于等于 1")
    return version


def parse_granularity(value: Any) -> int:
    if value is None or str(value).strip() == "":
        return 1
    text = str(value).strip().lower()
    text = text.removesuffix("秒") or text
    text = text.removesuffix("s") or text
    try:
        granularity = int(float(text))
    except ValueError as exc:
        raise BandwidthError(f"限速粒度「{value}」必须是正整数秒") from exc
    if granularity < 1:
        raise BandwidthError("限速粒度必须是正整数秒")
    return granularity


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in TRUE_VALUES


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _iso(moment: datetime | None = None) -> str:
    return (moment or _now()).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# 业务服务
# ---------------------------------------------------------------------------


class BandwidthService:
    """带宽策略生命周期与节点配额唯一性。"""

    def __init__(self) -> None:
        # 并发启用同一节点的多条策略时，靠这把锁保证执行结果只有一条。
        self._lock = threading.RLock()

    # -- 列表与明细 ---------------------------------------------------------

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        node: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if node:
            rows = [row for row in rows if str(row.get("节点标识", "")).strip() == node.strip()]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记策略：节点标识、限速参数、生效时段与版本都要落库。"""
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, [f"缺少必填字段：{'、'.join(missing)}"]
        try:
            download = parse_rate(values.get("下载带宽上限"), "下载带宽上限")
            upload = parse_rate(values.get("上传带宽上限"), "上传带宽上限")
            burst = parse_rate(values.get("突发额度"), "突发额度", allow_zero=True) or {
                "原值": "0",
                "Mbps": 0.0,
                "显示": "0Mbps",
            }
            granularity = parse_granularity(values.get("限速粒度秒"))
            priority = parse_priority(values.get("优先级权重"))
            version = parse_version(values.get("策略版本"))
            window = parse_window(values.get("生效时段"))
        except BandwidthError as exc:
            return None, [str(exc)]
        if download is None:
            return None, ["下载带宽上限不能为空"]

        rows = store.rows(MODULE)
        policy_id = str(values.get("策略编号")).strip()
        if any(str(row.get("策略编号") or "").strip() == policy_id for row in rows):
            return None, [f"策略编号 {policy_id} 已存在，请改用版本号登记新版本"]
        entry = {"id": store.next_id(rows)}
        for field in POLICY_FIELDS:
            entry[field] = values.get(field)
        entry.update(
            {
                "策略编号": policy_id,
                "节点标识": str(values.get("节点标识")).strip(),
                "下载带宽上限": download["原值"],
                "下载带宽上限Mbps": download["Mbps"],
                "上传带宽上限": (upload or {}).get("原值", ""),
                "上传带宽上限Mbps": (upload or {}).get("Mbps"),
                "突发额度": burst["原值"],
                "突发额度Mbps": burst["Mbps"],
                "限速粒度秒": granularity,
                "优先级权重": values.get("优先级权重") or "0",
                "优先级数值": priority,
                "生效时段": window.text,
                "策略版本": version,
                "唯一配额": self._quota_text(
                    download["Mbps"],
                    burst["Mbps"],
                    (upload or {}).get("Mbps"),
                    granularity,
                ),
                "status": STATUS_ORDER[0],
                "pending": True,
                "abnormal": False,
            }
        )
        rows.append(entry)
        return entry, []

    # -- 生命周期 -----------------------------------------------------------

    def run_action(
        self,
        entry_id: int,
        action: str,
        options: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """执行生命周期动作。

        ``options`` 里可以带 ``at``（校准时间）与 ``模拟落库失败``（演练切换失败），
        后者用来验证“新策略落库失败时旧配额继续生效”。
        """
        options = options or {}
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"带宽策略 {entry_id} 不存在或已归档"
        rule = ACTION_RULES.get(action)
        if rule is None:
            return None, f"动作「{action}」不属于带宽控制可执行范围"
        allowed_from, _target = rule
        current = str(entry.get("status") or "")
        if current not in allowed_from:
            if action == "启用策略" and current in {"草稿", "已暂停"}:
                return None, "策略尚未完成评审，不能直接启用；请先提交策略评审（暂停后恢复需重新评审）"
            if action == "提交策略评审" and current == "已启用":
                return None, "策略已启用，无需重复提交评审"
            return None, f"策略当前状态「{current}」不允许执行「{action}」"

        try:
            at = self._moment(options.get("at"))
        except BandwidthError as exc:
            return None, str(exc)

        if action in {"提交策略评审", "恢复启用", "重新评审"}:
            previous = current
            entry["status"] = "策略评审中"
            entry["pending"] = True
            entry["abnormal"] = False
            entry["评审提交时间"] = _iso(at)
            if previous == "已暂停":
                entry["恢复评审说明"] = "暂停后恢复必须重新评审，评审通过后才能重新参与带宽分配"
                return entry, "带宽策略已暂停，恢复需重新评审，已提交策略评审"
            return entry, "带宽策略已提交评审"

        if action == "暂停策略":
            return self._pause(entry, at, options)
        if action == "启用策略":
            return self._enable(entry, at, options)
        return None, f"动作「{action}」暂未实现"

    def _enable(
        self,
        entry: dict[str, Any],
        at: datetime,
        options: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        node = str(entry.get("节点标识") or "").strip()
        if not node:
            return None, "策略缺少节点标识，不能参与带宽分配"
        try:
            policy = self._normalize_policy(entry, at)
        except BandwidthError as exc:
            return None, f"带宽策略校验失败：{exc}；旧配额继续生效"

        simulate_failure = _truthy(
            options.get("模拟落库失败") or options.get("simulate_persist_failure")
        )
        with self._lock:
            before = store.snapshot()
            try:
                if simulate_failure:
                    raise BandwidthError("模拟策略落库失败")
                result = self._apply_policy(policy, at, source="启用策略")
            except BandwidthError as exc:
                store.restore(before)
                return None, f"带宽策略切换失败：{exc}；旧配额继续生效"
            except Exception as exc:  # pragma: no cover - 防御性回滚
                store.restore(before)
                return None, f"带宽策略切换失败：{exc}；旧配额继续生效"

            entry["status"] = "已启用"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["启用时间"] = _iso(at)
            entry["最近分配编号"] = result["allocation"]["分配编号"]
            entry["分配版本号"] = result["allocation"]["版本号"]
            entry["限速执行编号"] = result["execution"].get("执行编号")
            entry["唯一配额"] = result["allocation"]["唯一配额"]
            return entry, result["message"]

    def _pause(
        self,
        entry: dict[str, Any],
        at: datetime,
        options: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        node = str(entry.get("节点标识") or "").strip()
        with self._lock:
            before = store.snapshot()
            try:
                entry["status"] = "已暂停"
                entry["pending"] = True
                entry["abnormal"] = False
                entry["暂停时间"] = _iso(at)
                # 该策略名下的生效分配先降级；随后让节点重新挑选赢家，
                # 没有后继策略时沿用上一配额，已有下载不会失速。
                for allocation in store.allocations():
                    if (
                        allocation.get("节点标识") == node
                        and allocation.get("策略编号") == entry.get("策略编号")
                        and allocation.get("状态") == "生效"
                    ):
                        allocation["状态"] = "已暂停"
                        allocation["状态说明"] = "策略已暂停，执行结果沿用上一配额"
                        allocation["失效时间"] = _iso(at)
                self.reconcile_node(node, at)
                execution = self._execution_for(node)
                if execution is not None:
                    self._writeback_node(node, at, execution=execution)
            except Exception as exc:  # pragma: no cover - 防御性回滚
                store.restore(before)
                return None, f"带宽策略暂停失败：{exc}；旧配额继续生效"
        return entry, "带宽策略已暂停；恢复需重新评审，已有下载继续按原配额限速"

    # -- 分配与执行 ---------------------------------------------------------

    def _normalize_policy(self, entry: dict[str, Any], at: datetime) -> dict[str, Any]:
        download = parse_rate(entry.get("下载带宽上限"), "下载带宽上限")
        if download is None:
            raise BandwidthError("下载带宽上限不能为空")
        upload = parse_rate(entry.get("上传带宽上限"), "上传带宽上限")
        burst = parse_rate(entry.get("突发额度"), "突发额度", allow_zero=True) or {
            "原值": "0",
            "Mbps": 0.0,
            "显示": "0Mbps",
        }
        granularity = parse_granularity(entry.get("限速粒度秒"))
        priority = parse_priority(entry.get("优先级权重"))
        version = parse_version(entry.get("策略版本"))
        window = parse_window(entry.get("生效时段"))
        node = str(entry.get("节点标识") or "").strip()
        policy_id = str(entry.get("策略编号") or "").strip()
        if not node or not policy_id:
            raise BandwidthError("策略编号与节点标识都不能为空")

        # 同一条策略的版本必须唯一，避免同一个版本号对应两套限速参数。
        for other in store.rows(MODULE):
            if int(other.get("id", 0)) == int(entry.get("id", 0)):
                continue
            if (
                str(other.get("节点标识") or "").strip() == node
                and str(other.get("策略编号") or "").strip() == policy_id
                and parse_version(other.get("策略版本")) == version
            ):
                raise BandwidthError(
                    f"策略 {policy_id} 在节点 {node} 的版本 {version} 已存在，请登记新版本"
                )

        normalized = copy.deepcopy(entry)
        normalized.update(
            {
                "节点标识": node,
                "策略编号": policy_id,
                "下载带宽上限": download["原值"],
                "下载带宽上限Mbps": download["Mbps"],
                "上传带宽上限": (upload or {}).get("原值", ""),
                "上传带宽上限Mbps": (upload or {}).get("Mbps"),
                "突发额度": burst["原值"],
                "突发额度Mbps": burst["Mbps"],
                "限速粒度秒": granularity,
                "优先级数值": priority,
                "策略版本": version,
                "生效时段": window.text,
                "唯一配额": self._quota_text(
                    download["Mbps"],
                    burst["Mbps"],
                    (upload or {}).get("Mbps"),
                    granularity,
                ),
                "生效中": window.covers(at),
            }
        )
        return normalized

    def _apply_policy(
        self,
        policy: dict[str, Any],
        at: datetime,
        *,
        source: str,
    ) -> dict[str, Any]:
        """把一条策略纳入节点分配，并保证节点只有一条执行结果。

        调用方必须已经持有 ``self._lock``（或在副本上演练）。
        """
        node = policy["节点标识"]
        candidates = self._candidates(node, at)
        if policy["生效中"] and policy["策略编号"] not in {item["策略编号"] for item in candidates}:
            # 当前策略刚启用、库里状态可能还没来得及改，显式并入候选。
            candidates = candidates + [policy]
        winner = self._pick_winner(candidates)
        is_winner = bool(
            winner
            and winner.get("策略编号") == policy["策略编号"]
            and parse_version(winner.get("策略版本")) == policy["策略版本"]
            and policy["生效中"]
        )
        conflict = len(candidates) > 1

        if not policy["生效中"]:
            allocation = self._record_allocation(
                policy,
                at,
                status="待生效",
                winner=winner,
                source=source,
                conflict=conflict,
            )
            execution = self._execution_for(node)
            if execution is not None:
                current = self._carry_over(
                    node,
                    at,
                    reason="策略尚未进入生效时段，沿用上一有效配额",
                    execution=execution,
                )
                self._writeback_node(node, at, allocation=current, execution=execution)
                return {
                    "allocation": allocation,
                    "execution": execution,
                    "message": (
                        f"策略 {policy['策略编号']} 已登记分配，生效时段未到；"
                        f"节点 {node} 仍按当前唯一配额执行"
                    ),
                }
            return {
                "allocation": allocation,
                "execution": {},
                "message": f"策略 {policy['策略编号']} 已登记分配，生效时段未到，尚未生成限速执行结果",
            }

        if not is_winner:
            allocation = self._record_allocation(
                policy,
                at,
                status="冲突落选",
                winner=winner,
                source=source,
                conflict=conflict,
            )
            execution = self._execution_for(node)
            if execution is not None:
                current = self._carry_over(
                    node,
                    at,
                    reason=f"与更高优先级策略 {winner['策略编号']} 冲突，节点沿用唯一生效配额",
                    execution=execution,
                )
                self._writeback_node(node, at, allocation=current, execution=execution)
            return {
                "allocation": allocation,
                "execution": execution or {},
                "message": (
                    f"策略 {policy['策略编号']} 已登记分配，但节点 {node} 由更高优先级策略 "
                    f"{winner['策略编号']} 生效，未叠加额度"
                ),
            }

        allocation = self._record_allocation(
            policy,
            at,
            status="生效",
            winner=winner,
            source=source,
            conflict=conflict,
        )
        execution = self._upsert_execution(node, allocation, at, reason=source)
        self._writeback_node(node, at, allocation=allocation, execution=execution)
        return {
            "allocation": allocation,
            "execution": execution,
            "message": (
                f"节点 {node} 已按策略 {policy['策略编号']} 启用限速，"
                f"唯一配额 {allocation['唯一配额']}"
            ),
        }

    def _candidates(self, node: str, at: datetime) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for row in store.rows(MODULE):
            if str(row.get("节点标识") or "").strip() != node:
                continue
            if row.get("status") != "已启用":
                continue
            try:
                window = parse_window(row.get("生效时段"))
                if not window.covers(at):
                    continue
                parse_rate(row.get("下载带宽上限"), "下载带宽上限")
                parse_priority(row.get("优先级权重"))
                parse_version(row.get("策略版本"))
            except BandwidthError:
                continue
            candidates.append(copy.deepcopy(row))
        return candidates

    @staticmethod
    def _pick_winner(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                parse_priority(item.get("优先级权重")),
                parse_version(item.get("策略版本")),
                int(item.get("id", 0)),
            ),
        )

    def _record_allocation(
        self,
        policy: dict[str, Any],
        at: datetime,
        *,
        status: str,
        winner: dict[str, Any] | None,
        source: str,
        conflict: bool,
    ) -> dict[str, Any]:
        allocations = store.allocations()
        node = policy["节点标识"]

        # 幂等键：同一节点的同一策略版本、同一组限速参数重复提交只保留一条分配。
        idempotent_key = (
            node,
            policy["策略编号"],
            policy["策略版本"],
            policy["下载带宽上限"],
            policy["突发额度"],
            policy["生效时段"],
            policy["限速粒度秒"],
        )
        existing = next(
            (item for item in allocations if item.get("幂等键") == idempotent_key),
            None,
        )
        if existing is not None:
            existing["重复提交次数"] = int(existing.get("重复提交次数", 0)) + 1
            existing["最近提交时间"] = _iso(at)
            if status == "生效":
                if existing.get("状态") != "生效":
                    # 待生效/落选版本真正转为生效，算一次新的分配版本。
                    existing["版本号"] = max(
                        (
                            int(item.get("版本号", 0))
                            for item in allocations
                            if item.get("节点标识") == node
                        ),
                        default=0,
                    ) + 1
                    existing["生成时间"] = _iso(at)
                self._expire_other_allocations(allocations, existing, node, policy, at)
                existing["状态"] = "生效"
                existing["状态说明"] = self._status_note(status, winner, conflict)
                existing["是否生效"] = True
                existing["失效时间"] = None
            elif existing.get("状态") not in ALLOCATION_ACTIVE:
                existing["状态"] = status
                existing["状态说明"] = self._status_note(status, winner, conflict)
                existing["是否生效"] = False
            return existing

        if status == "生效":
            self._expire_other_allocations(allocations, None, node, policy, at)

        version = max(
            (int(item.get("版本号", 0)) for item in allocations if item.get("节点标识") == node),
            default=0,
        ) + 1
        allocation = {
            "id": store.next_id(allocations),
            "分配编号": f"NBA-{store.next_id(allocations):04d}",
            "节点标识": node,
            "策略编号": policy["策略编号"],
            "策略版本": policy["策略版本"],
            "优先级权重": policy.get("优先级权重") or "0",
            "优先级数值": policy.get("优先级数值", 0),
            "下载带宽上限": policy["下载带宽上限"],
            "上传带宽上限": policy.get("上传带宽上限") or "",
            "突发额度": policy["突发额度"],
            "限速粒度秒": policy["限速粒度秒"],
            "生效时段": policy["生效时段"],
            "下载带宽上限Mbps": policy["下载带宽上限Mbps"],
            "上传带宽上限Mbps": policy.get("上传带宽上限Mbps"),
            "突发额度Mbps": policy["突发额度Mbps"],
            "唯一配额": policy["唯一配额"],
            "版本号": version,
            "状态": status,
            "状态说明": self._status_note(status, winner, conflict),
            "冲突策略": sorted(
                {
                    str(item.get("策略编号"))
                    for item in self._candidates(node, at)
                    if str(item.get("策略编号")) != policy["策略编号"]
                }
            ),
            "是否生效": status in ALLOCATION_ACTIVE,
            "策略快照": self._snapshot_policy(policy, at),
            "数据来源": source,
            "生成时间": _iso(at),
            "最近提交时间": _iso(at),
            "幂等键": idempotent_key,
            "重复提交次数": 0,
        }
        allocations.append(allocation)
        return allocation

    @staticmethod
    def _expire_other_allocations(
        allocations: list[dict[str, Any]],
        keep: dict[str, Any] | None,
        node: str,
        policy: dict[str, Any],
        at: datetime,
    ) -> None:
        """新版本生效时把节点上其他生效/沿用版本降级，保证只有一条当前分配。"""
        for allocation in allocations:
            if allocation is keep or allocation.get("节点标识") != node:
                continue
            if allocation.get("状态") not in ALLOCATION_ACTIVE:
                continue
            allocation["状态"] = "被取代"
            allocation["状态说明"] = (
                f"被策略 {policy['策略编号']} v{policy['策略版本']} 取代"
            )
            allocation["是否生效"] = False
            allocation["失效时间"] = _iso(at)

    @staticmethod
    def _status_note(status: str, winner: dict[str, Any] | None, conflict: bool) -> str:
        if status == "生效":
            return "策略在生效时段内且优先级最高" if not conflict else "多条策略冲突，按优先级胜出，额度不叠加"
        if status == "冲突落选":
            return f"与同节点更高优先级策略 {winner.get('策略编号') if winner else '—'} 冲突，未叠加额度"
        if status == "待生效":
            return "策略尚未进入生效时段"
        if status == "沿用":
            return "沿用上一有效配额"
        return "已登记"

    def _upsert_execution(
        self,
        node: str,
        allocation: dict[str, Any],
        at: datetime,
        *,
        reason: str,
    ) -> dict[str, Any]:
        """同一节点只保留一条限速执行结果；切换时原地更新并留档旧值。"""
        executions = store.executions()
        execution = self._execution_for(node)
        same_quota = (
            execution is not None
            and execution.get("策略编号") == allocation["策略编号"]
            and execution.get("策略版本") == allocation["策略版本"]
            and execution.get("分配编号") == allocation["分配编号"]
            and execution.get("唯一配额") == allocation["唯一配额"]
            and execution.get("执行状态") != "沿用"
        )
        if execution is None:
            execution = {
                "id": store.next_id(executions),
                "执行编号": f"BEX-{store.next_id(executions):04d}",
                "节点标识": node,
                "节点状态": "在线",
                "切换次数": 0,
                "实际下载流量字节": 0,
                "实际下载流量MB": 0,
                "下载任务数": 0,
                "首次生效时间": _iso(at),
                "唯一限速键": f"{node}|{allocation['突发额度']}",
            }
            executions.append(execution)
        elif not same_quota:
            # 切换前留档旧值，保证“节点离线/切换后仍能追溯当时的执行记录”。
            self._append_history(
                execution,
                event="策略切换",
                at=at,
                reason=reason,
                note=f"由 {execution.get('策略编号')} 切换为 {allocation['策略编号']}",
            )
            execution["切换次数"] = int(execution.get("切换次数", 0)) + 1
            execution["上一策略编号"] = execution.get("策略编号")
            execution["上一唯一配额"] = execution.get("唯一配额")

        execution.update(
            {
                "节点标识": node,
                "策略编号": allocation["策略编号"],
                "策略版本": allocation["策略版本"],
                "分配编号": allocation["分配编号"],
                "分配版本": allocation["版本号"],
                "下载带宽上限": allocation["下载带宽上限"],
                "上传带宽上限": allocation.get("上传带宽上限") or "",
                "突发额度": allocation["突发额度"],
                "限速粒度秒": allocation["限速粒度秒"],
                "优先级权重": allocation.get("优先级权重") or "0",
                "优先级数值": allocation.get("优先级数值", 0),
                "下载带宽上限Mbps": allocation["下载带宽上限Mbps"],
                "上传带宽上限Mbps": allocation.get("上传带宽上限Mbps"),
                "突发额度Mbps": allocation["突发额度Mbps"],
                "唯一配额": allocation["唯一配额"],
                "唯一限速键": f"{node}|{allocation['突发额度']}",
                "限速规则": (
                    f"下载 {allocation['下载带宽上限']}，突发 {allocation['突发额度']}，"
                    f"粒度 {allocation['限速粒度秒']} 秒"
                ),
                "执行状态": "沿用" if allocation["状态"] == "沿用" else "生效",
                "最近生效时间": _iso(at),
                "更新时间": _iso(at),
                "策略快照": copy.deepcopy(allocation["策略快照"]),
            }
        )
        return execution

    def _append_history(
        self,
        execution: dict[str, Any],
        *,
        event: str,
        at: datetime,
        reason: str,
        note: str = "",
    ) -> dict[str, Any]:
        history = store.execution_history()
        record = {
            "id": store.next_id(history),
            "留档编号": f"BEH-{store.next_id(history):04d}",
            "执行编号": execution.get("执行编号"),
            "节点标识": execution.get("节点标识"),
            "事件": event,
            "事件说明": reason,
            "备注": note,
            "节点状态": execution.get("节点状态", "在线"),
            "策略编号": execution.get("策略编号"),
            "策略版本": execution.get("策略版本"),
            "分配编号": execution.get("分配编号"),
            "分配版本": execution.get("分配版本"),
            "下载带宽上限": execution.get("下载带宽上限"),
            "突发额度": execution.get("突发额度"),
            "限速粒度秒": execution.get("限速粒度秒"),
            "唯一配额": execution.get("唯一配额"),
            "策略快照": copy.deepcopy(execution.get("策略快照") or {}),
            "留档时间": _iso(at),
        }
        history.append(record)
        return record

    def _carry_over(
        self,
        node: str,
        at: datetime,
        *,
        reason: str,
        execution: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """空窗期沿用上一有效配额，并生成新的分配版本记录。"""
        if execution is None:
            return None
        current = self._current_allocation(node)
        if current is not None and self._policy_still_active(
            str(current.get("策略编号") or ""), int(current.get("策略版本", 0) or 0), at
        ):
            # 当前配额仍然合法（对应策略仍已启用且在生效时段内），不需要降级沿用。
            return current
        if current and current.get("状态") == "沿用":
            current["最近提交时间"] = _iso(at)
            return current

        allocations = store.allocations()
        if current and current.get("状态") == "生效":
            current["状态"] = "已过期"
            current["状态说明"] = reason
            current["失效时间"] = _iso(at)

        version = max(
            (int(item.get("版本号", 0)) for item in allocations if item.get("节点标识") == node),
            default=0,
        ) + 1
        allocation = {
            "id": store.next_id(allocations),
            "分配编号": f"NBA-{store.next_id(allocations):04d}",
            "节点标识": node,
            "策略编号": execution.get("策略编号"),
            "策略版本": execution.get("策略版本", 1),
            "优先级权重": execution.get("优先级权重", "0"),
            "优先级数值": execution.get("优先级数值", 0),
            "下载带宽上限": execution.get("下载带宽上限"),
            "上传带宽上限": execution.get("上传带宽上限") or "",
            "突发额度": execution.get("突发额度") or "0",
            "限速粒度秒": execution.get("限速粒度秒") or 1,
            "生效时段": "沿用上一版本",
            "下载带宽上限Mbps": execution.get("下载带宽上限Mbps", 0),
            "上传带宽上限Mbps": execution.get("上传带宽上限Mbps"),
            "突发额度Mbps": execution.get("突发额度Mbps", 0),
            "唯一配额": execution.get("唯一配额"),
            "版本号": version,
            "状态": "沿用",
            "状态说明": reason,
            "冲突策略": [],
            "是否生效": True,
            "策略快照": copy.deepcopy(execution.get("策略快照") or {}),
            "数据来源": "空窗沿用",
            "生成时间": _iso(at),
            "最近提交时间": _iso(at),
            "幂等键": (node, execution.get("策略编号"), execution.get("策略版本"), "carry-over"),
            "重复提交次数": 0,
        }
        allocations.append(allocation)
        execution.update(
            {
                "分配编号": allocation["分配编号"],
                "分配版本": allocation["版本号"],
                "执行状态": "沿用",
                "更新时间": _iso(at),
            }
        )
        self._append_history(
            execution,
            event="配额沿用",
            at=at,
            reason=reason,
            note="生效时段空窗，保留上一有效配额，避免节点短暂无合法分配",
        )
        return allocation

    def _policy_still_active(self, policy_id: str, version: int, at: datetime) -> bool:
        for row in store.rows(MODULE):
            if str(row.get("策略编号") or "").strip() != policy_id:
                continue
            if parse_version(row.get("策略版本")) != version:
                continue
            if row.get("status") != "已启用":
                return False
            try:
                return parse_window(row.get("生效时段")).covers(at)
            except BandwidthError:
                return False
        return False

    def _execution_for(self, node: str) -> dict[str, Any] | None:
        matches = [item for item in store.executions() if item.get("节点标识") == node]
        if not matches:
            return None
        # 防御：历史数据若出现多条，保留最新一条，其余留档后移除。
        matched = max(matches, key=lambda item: int(item.get("id", 0)))
        for extra in matches:
            if extra is matched:
                continue
            self._append_history(
                extra,
                event="重复执行结果清理",
                at=_now(),
                reason="同一节点只允许一条限速执行结果，重复记录已留档",
            )
            store.executions().remove(extra)
        return matched

    def _current_allocation(self, node: str) -> dict[str, Any] | None:
        active = [
            item
            for item in store.allocations()
            if item.get("节点标识") == node and item.get("状态") in ALLOCATION_ACTIVE
        ]
        if not active:
            return None
        return max(active, key=lambda item: int(item.get("版本号", 0)))

    # -- 时段校准、离线留档、清单回写 ---------------------------------------

    def reconcile_node(self, node: str, at: datetime | None = None) -> dict[str, Any] | None:
        """按当前时间校准节点配额。

        到期的策略降级，尚未进入生效时段的低优先级策略不会让节点失去配额：
        空窗期沿用上一有效版本；低优先级策略进入时段后再切换。
        """
        at = self._moment(at)
        node = str(node or "").strip()
        if not node:
            return None
        with self._lock:
            execution = self._execution_for(node)
            candidates = self._candidates(node, at)
            winner = self._pick_winner(candidates)
            if winner is None:
                if execution is not None:
                    allocation = self._carry_over(
                        node,
                        at,
                        reason="高优先级策略已到期且后继策略未进入生效时段，沿用上一有效配额",
                        execution=execution,
                    )
                    self._writeback_node(node, at, allocation=allocation, execution=execution)
                    return execution
                return None

            if (
                execution is not None
                and execution.get("策略编号") == winner.get("策略编号")
                and execution.get("策略版本") == parse_version(winner.get("策略版本"))
                and execution.get("执行状态") != "沿用"
            ):
                self._writeback_node(node, at)
                return execution

            policy = self._normalize_policy(winner, at)
            allocation = self._record_allocation(
                policy,
                at,
                status="生效",
                winner=policy,
                source="生效时段校准",
                conflict=len(candidates) > 1,
            )
            refresh = self._upsert_execution(node, allocation, at, reason="生效时段校准")
            self._writeback_node(node, at, allocation=allocation, execution=refresh)
            return refresh

    def reconcile_all(self, at: datetime | None = None) -> list[dict[str, Any]]:
        at = self._moment(at)
        nodes = {
            str(row.get("节点标识") or "").strip()
            for row in store.rows(MODULE)
            if row.get("status") == "已启用"
        }
        results: list[dict[str, Any]] = []
        for node in sorted(item for item in nodes if item):
            execution = self.reconcile_node(node, at)
            if execution:
                results.append(execution)
        return results

    def handle_node_offline(self, node: str, at: datetime | None = None) -> dict[str, Any] | None:
        """节点离线只把执行结果标记为离线并留档，不删除当时的带宽执行记录。"""
        at = self._moment(at)
        node = str(node or "").strip()
        if not node:
            return None
        with self._lock:
            execution = self._execution_for(node)
            if execution is None:
                return None
            # 先按当前时间校准，确保留档的是离线那一刻真正生效的配额。
            self.reconcile_node(node, at)
            execution = self._execution_for(node)
            if execution is None:
                return None
            if execution.get("节点状态") != "离线":
                execution["节点状态"] = "离线"
                execution["离线时间"] = _iso(at)
                execution["执行状态"] = "离线保留"
                self._append_history(
                    execution,
                    event="节点离线",
                    at=at,
                    reason="节点离线后保留离线时的带宽执行记录与配额",
                    note="执行结果不删除，便于追溯离线时实际执行的限速",
                )
            self._writeback_node(node, at, execution=execution)
            return execution

    def handle_node_online(self, node: str, at: datetime | None = None) -> dict[str, Any] | None:
        at = self._moment(at)
        node = str(node or "").strip()
        with self._lock:
            execution = self._execution_for(node)
            if execution is None:
                return None
            if execution.get("节点状态") != "在线":
                execution["节点状态"] = "在线"
                execution["上线时间"] = _iso(at)
                self._append_history(
                    execution,
                    event="节点上线",
                    at=at,
                    reason="节点恢复在线，继续沿用离线前的带宽执行记录",
                )
            self.reconcile_node(node, at)
            self._writeback_node(node, at)
            return self._execution_for(node)

    def _writeback_node(
        self,
        node: str,
        at: datetime | None = None,
        *,
        allocation: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
    ) -> None:
        """把节点带宽分配回写到集群节点侧清单，覆盖旧配额而不是叠加。"""
        allocation = allocation or self._current_allocation(node)
        execution = execution or self._execution_for(node)
        if allocation is None and execution is None:
            return
        for worker in store.rows("worker"):
            if str(worker.get("节点编号") or "").strip() != node:
                continue
            if allocation is not None:
                worker.update(
                    {
                        "带宽分配编号": allocation.get("分配编号", ""),
                        "带宽分配版本": allocation.get("版本号", ""),
                        "带宽分配状态": allocation.get("状态", ""),
                        "下载带宽上限": allocation.get("下载带宽上限", ""),
                        "上传带宽上限": allocation.get("上传带宽上限", "") or "不限",
                        "突发额度": allocation.get("突发额度", ""),
                        "限速粒度秒": allocation.get("限速粒度秒", ""),
                        "唯一配额": allocation.get("唯一配额", ""),
                        "带宽策略编号": allocation.get("策略编号", ""),
                        "带宽策略版本": allocation.get("策略版本", ""),
                        "带宽策略快照": copy.deepcopy(allocation.get("策略快照") or {}),
                        "带宽更新时间": _iso(at),
                    }
                )
            if execution is not None:
                worker["限速执行编号"] = execution.get("执行编号", "")
                worker["限速执行状态"] = execution.get("执行状态", "")
                worker["节点在线状态"] = execution.get("节点状态", "")
                worker["带宽实际流量MB"] = execution.get("实际下载流量MB", 0)
            # 旧版本遗留的冲突限额字段统一清理，避免采集节点列表出现两套限额。
            worker.pop("第二条限额", None)
            worker.pop("冲突限额", None)

    def sync_manifests(self, node_rows: list[dict[str, Any]], at: datetime | None = None) -> None:
        """采集节点列表读取前对齐配额清单，避免旧配额残留。"""
        at = self._moment(at)
        for row in node_rows:
            node = str(row.get("节点编号") or "").strip()
            if not node:
                continue
            with self._lock:
                self.reconcile_node(node, at)
                self._writeback_node(node, at)

    # -- 批量提交与查询 -----------------------------------------------------

    def submit_allocations(
        self,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        """同一节点的多条带宽记录一次提交整组，逐条给出唯一配额结果。

        组内每条策略都有自己的分配结果；节点级限速执行结果仍然只有一条。
        """
        try:
            at = self._moment(values.get("at"))
        except BandwidthError as exc:
            return None, str(exc)
        node = str(values.get("节点标识") or values.get("node") or "").strip()
        policy_ids = values.get("策略编号") or values.get("policy_ids") or values.get("策略编号列表")
        inline = values.get("策略") or values.get("policies") or []
        auto_review = _truthy(values.get("自动评审") or values.get("auto_review"))

        if isinstance(policy_ids, str):
            policy_ids = [item.strip() for item in re.split(r"[,，、\s]+", policy_ids) if item.strip()]
        policy_ids = list(policy_ids or [])

        if not node and policy_ids:
            first = next(
                (row for row in store.rows(MODULE) if row.get("策略编号") == policy_ids[0]),
                None,
            )
            node = str((first or {}).get("节点标识") or "").strip()
        if not node:
            return None, "批量提交节点带宽分配需要提供节点标识"

        with self._lock:
            # 整组提交在副本上计算，任何版本冲突/落库失败都回滚到提交前。
            before = store.snapshot()
            policies: list[dict[str, Any]] = []
            results: list[dict[str, Any]] = []
            if isinstance(inline, dict):
                inline = [inline]
            for item in inline:
                if not isinstance(item, dict):
                    continue
                payload = dict(item)
                payload.setdefault("节点标识", node)
                entry, errors = self.create_entry(payload)
                if entry is None:
                    results.append(
                        {
                            "策略编号": payload.get("策略编号"),
                            "ok": False,
                            "说明": "；".join(errors),
                            "唯一配额": None,
                        }
                    )
                    continue
                policies.append(entry)
            for policy_id in policy_ids:
                entry = next(
                    (row for row in store.rows(MODULE) if str(row.get("策略编号")) == str(policy_id)),
                    None,
                )
                if entry is None:
                    results.append(
                        {"策略编号": policy_id, "ok": False, "说明": "策略不存在", "唯一配额": None}
                    )
                    continue
                if str(entry.get("节点标识") or "").strip() != node:
                    results.append(
                        {
                            "策略编号": policy_id,
                            "ok": False,
                            "说明": f"策略不属于节点 {node}",
                            "唯一配额": None,
                        }
                    )
                    continue
                policies.append(entry)

            if not policies:
                store.restore(before)
                return None, "没有可提交的带宽策略记录"

            try:
                # 先做整体校验，任何一条版本冲突都不写库，保证整组原子性。
                normalized: list[dict[str, Any]] = []
                seen_versions: dict[int, str] = {}
                for policy in policies:
                    prepared = self._normalize_policy(policy, at)
                    version = prepared["策略版本"]
                    if version in seen_versions and seen_versions[version] != prepared["策略编号"]:
                        raise BandwidthError(
                            f"节点 {node} 的多个策略不能使用同一版本号 {version}"
                        )
                    seen_versions[version] = prepared["策略编号"]
                    normalized.append(prepared)

                for policy, prepared in zip(policies, normalized):
                    if policy.get("status") == "已启用":
                        pass
                    elif policy.get("status") == "策略评审中" or auto_review:
                        policy["status"] = "已启用"
                        policy["评审提交时间"] = _iso(at)
                        policy["启用时间"] = _iso(at)
                        policy["pending"] = False
                        policy["abnormal"] = False
                    else:
                        results.append(
                            {
                                "策略编号": prepared["策略编号"],
                                "策略版本": prepared["策略版本"],
                                "ok": False,
                                "唯一配额": prepared["唯一配额"],
                                "说明": "策略尚未完成评审，不能参与节点带宽分配",
                            }
                        )
                        continue

                    try:
                        outcome = self._apply_policy(
                            prepared,
                            at,
                            source="批量提交节点带宽分配",
                        )
                    except BandwidthError as exc:
                        results.append(
                            {
                                "策略编号": prepared["策略编号"],
                                "策略版本": prepared["策略版本"],
                                "ok": False,
                                "唯一配额": prepared["唯一配额"],
                                "说明": str(exc),
                            }
                        )
                        continue
                    allocation = outcome["allocation"]
                    results.append(
                        {
                            "策略编号": prepared["策略编号"],
                            "策略版本": prepared["策略版本"],
                            "ok": True,
                            "唯一配额": prepared["唯一配额"],
                            "分配编号": allocation.get("分配编号"),
                            "分配版本": allocation.get("版本号"),
                            "分配状态": allocation.get("状态"),
                            "是否节点生效配额": allocation.get("状态") == "生效",
                            "说明": outcome["message"],
                        }
                    )
                    policy["最近分配编号"] = allocation.get("分配编号")
                    policy["分配版本号"] = allocation.get("版本号")
                    policy["唯一配额"] = prepared["唯一配额"]
                execution = self._execution_for(node)
                allocation = self._current_allocation(node)
                self._writeback_node(node, at, allocation=allocation, execution=execution)
            except BandwidthError as exc:
                store.restore(before)
                return None, f"批量提交失败：{exc}；节点旧配额继续生效"
            except Exception as exc:  # pragma: no cover - 防御性回滚
                store.restore(before)
                return None, f"批量提交失败：{exc}；节点旧配额继续生效"

        execution = self._execution_for(node)
        allocation = self._current_allocation(node)
        succeeded = sum(1 for item in results if item.get("ok"))
        entry = {
            "节点标识": node,
            "分配编号": (allocation or {}).get("分配编号"),
            "分配版本": (allocation or {}).get("版本号"),
            "唯一配额": (allocation or {}).get("唯一配额") or (execution or {}).get("唯一配额"),
            "限速执行编号": (execution or {}).get("执行编号"),
            "执行结果条数": 1 if execution else 0,
            "逐条结果": results,
        }
        return entry, f"节点 {node} 整组提交完成：成功 {succeeded} 条，限速执行结果 1 条"

    def list_allocations(
        self,
        *,
        node: str | None = None,
        status: str | None = None,
        policy_id: str | None = None,
        only_current: bool = False,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.allocations())
        if node:
            rows = [row for row in rows if row.get("节点标识") == node]
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        if policy_id:
            rows = [row for row in rows if row.get("策略编号") == policy_id]
        if only_current:
            rows = [row for row in rows if row.get("状态") in ALLOCATION_ACTIVE]
        rows.sort(key=lambda row: (str(row.get("节点标识")), -int(row.get("版本号", 0))))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_executions(
        self,
        *,
        node: str | None = None,
        node_status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.executions())
        if node:
            rows = [row for row in rows if row.get("节点标识") == node]
        if node_status:
            rows = [row for row in rows if row.get("节点状态") == node_status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_history(
        self, *, node: str | None = None, page: int = 1, size: int = 20
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(reversed(store.execution_history()))
        if node:
            rows = [row for row in rows if row.get("节点标识") == node]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_traffic(
        self, *, node: str | None = None, page: int = 1, size: int = 20
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(reversed(store.traffic()))
        if node:
            rows = [row for row in rows if row.get("节点标识") == node]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def node_bandwidth(self, node: str, at: datetime | None = None) -> dict[str, Any] | None:
        """集群节点侧读取节点带宽分配与唯一限速执行结果。"""
        at = self._moment(at)
        node = str(node or "").strip()
        if not node:
            return None
        with self._lock:
            self.reconcile_node(node, at)
            self._writeback_node(node, at)
            execution = self._execution_for(node)
            allocation = self._current_allocation(node)
            if execution is None and allocation is None:
                return None
            worker = next(
                (
                    row
                    for row in store.rows("worker")
                    if str(row.get("节点编号") or "").strip() == node
                ),
                None,
            )
            return {
                "节点标识": node,
                "节点状态": (execution or {}).get("节点状态")
                or (worker or {}).get("节点在线状态")
                or (worker or {}).get("status"),
                "分配编号": (allocation or {}).get("分配编号"),
                "分配版本": (allocation or {}).get("版本号"),
                "分配状态": (allocation or {}).get("状态"),
                "策略编号": (allocation or {}).get("策略编号"),
                "策略版本": (allocation or {}).get("策略版本"),
                "下载带宽上限": (allocation or {}).get("下载带宽上限"),
                "上传带宽上限": (allocation or {}).get("上传带宽上限"),
                "突发额度": (allocation or {}).get("突发额度"),
                "限速粒度秒": (allocation or {}).get("限速粒度秒"),
                "唯一配额": (allocation or {}).get("唯一配额")
                or (execution or {}).get("唯一配额"),
                "策略快照": copy.deepcopy((allocation or {}).get("策略快照") or {}),
                "限速执行结果": execution,
                "分配版本总数": len(
                    [row for row in store.allocations() if row.get("节点标识") == node]
                ),
            }

    def report_traffic(
        self,
        node: str,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        """下载任务按分配到的配额执行，并把实际流量回写到执行结果。"""
        try:
            at = self._moment(values.get("at") or values.get("时间"))
        except BandwidthError as exc:
            return None, str(exc)
        node = str(node or values.get("节点标识") or "").strip()
        if not node:
            return None, "缺少节点标识"
        task_id = str(values.get("下载任务编号") or values.get("task_id") or "").strip()
        if not task_id:
            return None, "缺少下载任务编号"

        traffic_bytes = values.get("实际流量字节") or values.get("回写字节数")
        traffic_mb = values.get("实际流量MB") or values.get("回写MB")
        try:
            if traffic_bytes is not None:
                byte_count = int(float(traffic_bytes))
                mb_value = byte_count / (1024 * 1024)
            elif traffic_mb is not None:
                mb_value = float(traffic_mb)
                byte_count = int(mb_value * 1024 * 1024)
            else:
                return None, "缺少实际流量（实际流量MB 或 实际流量字节）"
        except (TypeError, ValueError):
            return None, "实际流量必须是数字"
        if byte_count < 0:
            return None, "实际流量不能为负数"
        elapsed = values.get("用时秒") or values.get("耗时秒")
        try:
            elapsed_seconds = float(elapsed) if elapsed is not None else 0.0
        except (TypeError, ValueError):
            return None, "用时秒必须是数字"

        with self._lock:
            bandwidth = self.node_bandwidth(node, at)
            execution = (bandwidth or {}).get("限速执行结果")
            allocation = self._current_allocation(node)
            if execution is None or allocation is None:
                return None, f"节点 {node} 没有可用的带宽分配，下载任务不能执行"
            quota = float(execution.get("下载带宽上限Mbps") or 0)
            burst = float(execution.get("突发额度Mbps") or 0)
            allowed = quota + burst
            actual_rate = (
                (byte_count * 8 / 1_000_000 / elapsed_seconds) if elapsed_seconds > 0 else 0.0
            )
            over_quota = allowed > 0 and actual_rate > allowed + 1e-9
            traffic = store.traffic()
            record = {
                "id": store.next_id(traffic),
                "流量编号": f"BTF-{store.next_id(traffic):04d}",
                "节点标识": node,
                "下载任务编号": task_id,
                "回写字节数": byte_count,
                "实际流量MB": round(mb_value, 3),
                "用时秒": elapsed_seconds,
                "实际速率Mbps": round(actual_rate, 3),
                "配额Mbps": quota,
                "突发额度Mbps": burst,
                "允许峰值Mbps": allowed,
                "是否超限": over_quota,
                "分配编号": allocation.get("分配编号"),
                "分配版本": allocation.get("版本号"),
                "执行编号": execution.get("执行编号"),
                "策略编号": allocation.get("策略编号"),
                "策略版本": allocation.get("策略版本"),
                "策略快照": copy.deepcopy(allocation.get("策略快照") or {}),
                "回写时间": _iso(at),
            }
            traffic.append(record)
            execution["实际下载流量字节"] = (
                int(execution.get("实际下载流量字节", 0)) + byte_count
            )
            execution["实际下载流量MB"] = round(
                execution["实际下载流量字节"] / (1024 * 1024), 3
            )
            execution["下载任务数"] = int(execution.get("下载任务数", 0)) + 1
            execution["最近回写时间"] = _iso(at)
            execution["最近实际速率Mbps"] = record["实际速率Mbps"]
            if over_quota:
                execution["超限次数"] = int(execution.get("超限次数", 0)) + 1
            self._writeback_node(node, at, allocation=allocation, execution=execution)
            return {
                "节点标识": node,
                "下载任务编号": task_id,
                "是否超限": over_quota,
                "实际速率Mbps": record["实际速率Mbps"],
                "唯一配额": allocation.get("唯一配额"),
                "配额Mbps": quota,
                "突发额度Mbps": burst,
                "分配编号": allocation.get("分配编号"),
                "执行编号": execution.get("执行编号"),
                "流量记录": record,
            }, ("下载任务流量已回写，实际速率超过配额" if over_quota else "下载任务流量已按配额回写")

    # -- 内部工具 -----------------------------------------------------------

    @staticmethod
    def _moment(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.replace(microsecond=0)
        text = str(value or "").strip()
        if not text:
            return _now()
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise BandwidthError(f"时间「{text}」无法解析") from exc
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed.replace(microsecond=0)

    @staticmethod
    def _quota_text(download: float, burst: float, upload: float | None, granularity: int) -> str:
        parts = [f"下载 {_number_text(download)}Mbps", f"突发 {_number_text(burst)}Mbps"]
        if upload:
            parts.append(f"上传 {_number_text(upload)}Mbps")
        parts.append(f"粒度 {granularity} 秒")
        return "，".join(parts)

    @staticmethod
    def _snapshot_policy(policy: dict[str, Any], at: datetime) -> dict[str, Any]:
        snapshot = {
            key: copy.deepcopy(value)
            for key, value in policy.items()
            if key not in {"策略快照", "幂等键"}
        }
        snapshot["快照时间"] = _iso(at)
        return snapshot


bandwidth_service = BandwidthService()
