"""带宽控制生命周期与冲突处置的回归测试（标准库 unittest，无额外依赖）。

运行：``cd backend && .venv/bin/python -m unittest discover -s tests -v``
"""
from __future__ import annotations

import threading
import unittest

from app.store import store
from app.services.bandwidth import BandwidthError, bandwidth_service, parse_rate, parse_window
from app.services.worker import WorkerService


class BandwidthTestBase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        # 清理单例里可能残留的锁状态之外的一切派生数据
        for name in ("bandwidth_allocation", "bandwidth_execution", "bandwidth_execution_history", "bandwidth_traffic"):
            store.derived(name).clear()

    def policies(self, node: str) -> list[dict]:
        return [row for row in store.rows("bandwidth") if row.get("节点标识") == node]

    def add_node(self, node: str = "WORK-0009") -> dict:
        row = {
            "id": store.next_id(store.rows("worker")),
            "status": "在线",
            "pending": False,
            "abnormal": False,
            "节点编号": node,
            "主机地址": "10.0.0.9",
            "并发槽位数": "8",
            "地域标签": "华东",
            "CPU使用率": "10%",
            "内存可用MB": "8000",
            "上次心跳时间": "2026-11-17",
            "节点状态标签": "在线",
        }
        store.rows("worker").append(row)
        return row

    def make_policy(self, **values) -> dict:
        entry, errors = bandwidth_service.create_entry(values)
        self.assertIsNotNone(entry, errors)
        return entry

    def approve_and_enable(self, entry: dict, at: str) -> dict:
        _entry, message = bandwidth_service.run_action(entry["id"], "提交策略评审", {"at": at})
        self.assertIn("评审", message)
        enabled, message = bandwidth_service.run_action(entry["id"], "启用策略", {"at": at})
        self.assertIsNotNone(enabled, message)
        return enabled


class ParsingTests(unittest.TestCase):
    def test_rate_units(self) -> None:
        self.assertEqual(parse_rate("100Mbps", "下载带宽上限")["Mbps"], 100.0)
        self.assertEqual(parse_rate("1Gbps", "下载带宽上限")["Mbps"], 1000.0)
        self.assertEqual(parse_rate("2000Kbps", "下载带宽上限")["Mbps"], 2.0)
        self.assertEqual(parse_rate("10MB/s", "下载带宽上限")["Mbps"], 80.0)
        self.assertEqual(parse_rate("100", "下载带宽上限")["Mbps"], 100.0)
        self.assertIsNone(parse_rate("", "下载带宽上限"))
        self.assertEqual(parse_rate("0", "突发额度", allow_zero=True)["Mbps"], 0.0)
        with self.assertRaises(BandwidthError):
            parse_rate("0", "下载带宽上限")
        with self.assertRaises(BandwidthError):
            parse_rate("很快", "下载带宽上限")

    def test_window_formats(self) -> None:
        self.assertTrue(parse_window("").always)
        self.assertTrue(parse_window("长期").always)
        daily = parse_window("09:00-18:00")
        self.assertIsNotNone(daily.daily_start)
        window = parse_window("2026-01-01~2026-12-31")
        from datetime import datetime

        self.assertTrue(window.covers(datetime(2026, 6, 1)))
        self.assertFalse(window.covers(datetime(2027, 1, 1)))
        with self.assertRaises(BandwidthError):
            parse_window("2026-12-31~2026-01-01")


class LifecycleTests(BandwidthTestBase):
    def test_review_required_before_enable(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9001",
            节点标识=node["节点编号"],
            下载带宽上限="100Mbps",
        )
        entry, message = bandwidth_service.run_action(policy["id"], "启用策略", {})
        self.assertIsNone(entry)
        self.assertIn("评审", message)

    def test_enable_builds_allocation_and_traceable_snapshot(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9001",
            节点标识=node["节点编号"],
            下载带宽上限="120Mbps",
            上传带宽上限="20Mbps",
            突发额度="8Mbps",
            限速粒度秒="2",
            优先级权重="高",
            生效时段="2026-11-01~2026-11-30",
            策略版本="3",
        )
        self.assertEqual(policy["策略版本"], 3)
        self.assertIn("下载 120Mbps", policy["唯一配额"])
        self.approve_and_enable(policy, "2026-11-10T10:00:00")

        bandwidth = bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-10T11:00:00")
        self.assertEqual(bandwidth["唯一配额"], "下载 120Mbps，突发 8Mbps，上传 20Mbps，粒度 2 秒")
        snapshot = bandwidth["策略快照"]
        self.assertEqual(snapshot["策略编号"], "BAND-9001")
        self.assertEqual(snapshot["策略版本"], 3)
        self.assertEqual(bandwidth["限速执行结果"]["唯一限速键"], f"{node['节点编号']}|8Mbps")
        self.assertEqual(len(store.allocations()), 1)
        self.assertEqual(len(store.executions()), 1)

    def test_pause_then_resume_requires_review_and_keeps_downloads(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9001",
            节点标识=node["节点编号"],
            下载带宽上限="100Mbps",
            生效时段="长期",
        )
        self.approve_and_enable(policy, "2026-11-10T10:00:00")
        before = bandwidth_service.node_bandwidth(node["节点编号"])["唯一配额"]

        entry, message = bandwidth_service.run_action(policy["id"], "暂停策略", {"at": "2026-11-10T11:00:00"})
        self.assertIsNotNone(entry)
        self.assertIn("重新评审", message)
        during_pause = bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-10T12:00:00")
        self.assertEqual(during_pause["唯一配额"], before)

        entry, message = bandwidth_service.run_action(policy["id"], "启用策略", {"at": "2026-11-10T12:00:00"})
        self.assertIsNone(entry)
        self.assertIn("重新评审", message)

        entry, message = bandwidth_service.run_action(policy["id"], "恢复启用", {"at": "2026-11-10T12:10:00"})
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "策略评审中")
        entry, _ = bandwidth_service.run_action(policy["id"], "启用策略", {"at": "2026-11-10T12:20:00"})
        self.assertIsNotNone(entry)
        self.assertEqual(
            bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-10T12:30:00")["唯一配额"],
            before,
        )
        self.assertEqual(len(store.executions()), 1)

    def test_switch_failure_keeps_old_quota(self) -> None:
        node = self.add_node()
        first = self.make_policy(
            策略编号="BAND-9001", 节点标识=node["节点编号"], 下载带宽上限="100Mbps", 优先级权重="10", 策略版本=1
        )
        second = self.make_policy(
            策略编号="BAND-9002", 节点标识=node["节点编号"], 下载带宽上限="50Mbps", 优先级权重="10", 策略版本=2
        )
        self.approve_and_enable(first, "2026-11-10T10:00:00")
        bandwidth_service.run_action(second["id"], "提交策略评审", {"at": "2026-11-10T10:00:00"})
        entry, message = bandwidth_service.run_action(
            second["id"], "启用策略", {"at": "2026-11-10T10:05:00", "模拟落库失败": True}
        )
        self.assertIsNone(entry)
        self.assertIn("旧配额继续生效", message)
        self.assertEqual(store.rows("bandwidth")[1].get("启用时间"), None)
        bandwidth = bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-10T10:10:00")
        self.assertEqual(bandwidth["唯一配额"], "下载 100Mbps，突发 0Mbps，粒度 1 秒")
        self.assertEqual(len(store.executions()), 1)

    def test_duplicate_enable_is_idempotent(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9001", 节点标识=node["节点编号"], 下载带宽上限="100Mbps", 生效时段="长期"
        )
        self.approve_and_enable(policy, "2026-11-10T10:00:00")
        first_quota = bandwidth_service.node_bandwidth(node["节点编号"])["唯一配额"]
        for _ in range(3):
            entry, _message = bandwidth_service.run_action(policy["id"], "启用策略", {"at": "2026-11-10T10:01:00"})
            self.assertIsNotNone(entry)
        allocations = [a for a in store.allocations() if a["节点标识"] == node["节点编号"]]
        self.assertEqual(len(allocations), 1)
        self.assertEqual(allocations[0]["重复提交次数"], 3)
        self.assertEqual(len(store.executions()), 1)
        self.assertEqual(bandwidth_service.node_bandwidth(node["节点编号"])["唯一配额"], first_quota)

    def test_concurrent_enable_yields_single_execution(self) -> None:
        node = self.add_node()
        first = self.make_policy(
            策略编号="BAND-9001", 节点标识=node["节点编号"], 下载带宽上限="100Mbps", 优先级权重="10", 策略版本=1
        )
        second = self.make_policy(
            策略编号="BAND-9002", 节点标识=node["节点编号"], 下载带宽上限="60Mbps", 优先级权重="5", 策略版本=2
        )
        for policy in (first, second):
            bandwidth_service.run_action(policy["id"], "提交策略评审", {"at": "2026-11-10T10:00:00"})

        def enable(index: int) -> None:
            entry_id = first["id"] if index % 2 == 0 else second["id"]
            bandwidth_service.run_action(entry_id, "启用策略", {"at": "2026-11-10T10:00:00"})

        threads = [threading.Thread(target=enable, args=(index,)) for index in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(store.executions()), 1)
        self.assertEqual(store.executions()[0]["唯一配额"], "下载 100Mbps，突发 0Mbps，粒度 1 秒")
        active = [
            a for a in store.allocations()
            if a["节点标识"] == node["节点编号"] and a["状态"] in ("生效", "沿用")
        ]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["策略编号"], "BAND-9001")

    def test_offline_keeps_execution_record(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9001", 节点标识=node["节点编号"], 下载带宽上限="100Mbps", 生效时段="长期"
        )
        self.approve_and_enable(policy, "2026-11-10T10:00:00")
        execution = bandwidth_service.handle_node_offline(node["节点编号"], "2026-11-10T11:00:00")
        self.assertIsNotNone(execution)
        self.assertEqual(execution["节点状态"], "离线")
        self.assertEqual(execution["唯一配额"], "下载 100Mbps，突发 0Mbps，粒度 1 秒")
        self.assertEqual(len(store.executions()), 1)
        self.assertTrue(any(item["事件"] == "节点离线" for item in store.execution_history()))
        # 再读一次也不能凭空补出第二条执行结果
        bandwidth = bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-10T12:00:00")
        self.assertEqual(bandwidth["节点状态"], "离线")
        self.assertEqual(len(store.executions()), 1)


class ConflictAndGapTests(BandwidthTestBase):
    def test_conflict_keeps_single_quota_and_no_stacking(self) -> None:
        node = self.add_node()
        high = self.make_policy(
            策略编号="BAND-9101",
            节点标识=node["节点编号"],
            下载带宽上限="200Mbps",
            突发额度="20Mbps",
            优先级权重="10",
            策略版本=1,
            生效时段="长期",
        )
        low = self.make_policy(
            策略编号="BAND-9102",
            节点标识=node["节点编号"],
            下载带宽上限="80Mbps",
            突发额度="5Mbps",
            优先级权重="5",
            策略版本=2,
            生效时段="长期",
        )
        self.approve_and_enable(high, "2026-11-10T10:00:00")
        self.approve_and_enable(low, "2026-11-10T10:01:00")

        execution = store.executions()[0]
        self.assertEqual(execution["唯一配额"], "下载 200Mbps，突发 20Mbps，粒度 1 秒")
        self.assertEqual(execution["唯一限速键"], f"{node['节点编号']}|20Mbps")
        low_allocations = [a for a in store.allocations() if a["策略编号"] == "BAND-9102"]
        self.assertEqual(low_allocations[0]["状态"], "冲突落选")
        self.assertIn("BAND-9101", low_allocations[0]["冲突策略"])
        self.assertNotIn("280", execution["唯一配额"])

    def test_gap_between_versions_carries_over(self) -> None:
        node = self.add_node()
        high = self.make_policy(
            策略编号="BAND-9201",
            节点标识=node["节点编号"],
            下载带宽上限="500Mbps",
            优先级权重="10",
            策略版本=1,
            生效时段="2026-11-01~2026-11-30",
        )
        low = self.make_policy(
            策略编号="BAND-9202",
            节点标识=node["节点编号"],
            下载带宽上限="200Mbps",
            突发额度="20Mbps",
            优先级权重="5",
            策略版本=2,
            生效时段="2026-12-02~2026-12-31",
        )
        self.approve_and_enable(high, "2026-11-15T10:00:00")
        self.approve_and_enable(low, "2026-11-15T10:01:00")

        during_high = bandwidth_service.node_bandwidth(node["节点编号"], "2026-11-15T12:00:00")
        self.assertEqual(during_high["唯一配额"], "下载 500Mbps，突发 0Mbps，粒度 1 秒")

        gap = bandwidth_service.node_bandwidth(node["节点编号"], "2026-12-01T00:00:00")
        self.assertEqual(gap["分配状态"], "沿用")
        self.assertEqual(gap["唯一配额"], "下载 500Mbps，突发 0Mbps，粒度 1 秒")

        after = bandwidth_service.node_bandwidth(node["节点编号"], "2026-12-02T00:00:01")
        self.assertEqual(after["分配状态"], "生效")
        self.assertEqual(after["唯一配额"], "下载 200Mbps，突发 20Mbps，粒度 1 秒")
        self.assertEqual(len(store.executions()), 1)

        versions = [a["版本号"] for a in store.allocations() if a["节点标识"] == node["节点编号"]]
        self.assertEqual(len(versions), len(set(versions)), "节点带宽分配版本号必须唯一")
        self.assertGreaterEqual(max(versions), 3)
        for allocation in store.allocations():
            self.assertIn("策略快照", allocation)

    def test_version_conflict_rejected(self) -> None:
        node = self.add_node()
        self.make_policy(
            策略编号="BAND-9301", 节点标识=node["节点编号"], 下载带宽上限="100Mbps", 策略版本=1
        )
        entry, errors = bandwidth_service.create_entry(
            {"策略编号": "BAND-9301", "节点标识": node["节点编号"], "下载带宽上限": "50Mbps", "策略版本": 1}
        )
        self.assertIsNone(entry)
        self.assertTrue(errors)
        # 同一策略换成新版本可以登记
        entry, errors = bandwidth_service.create_entry(
            {"策略编号": "BAND-9302", "节点标识": node["节点编号"], "下载带宽上限": "50Mbps", "策略版本": 2}
        )
        self.assertIsNotNone(entry, errors)


class BatchAndTrafficTests(BandwidthTestBase):
    def test_batch_submit_reports_each_policy_quota(self) -> None:
        node = self.add_node()
        for index, (rate, priority, version) in enumerate(
            [("300Mbps", "10", 1), ("100Mbps", "5", 2), ("50Mbps", "1", 3)], start=1
        ):
            policy = self.make_policy(
                策略编号=f"BAND-94{index:02d}",
                节点标识=node["节点编号"],
                下载带宽上限=rate,
                优先级权重=priority,
                策略版本=version,
                生效时段="长期",
            )
            bandwidth_service.run_action(policy["id"], "提交策略评审", {"at": "2026-11-10T10:00:00"})

        entry, message = bandwidth_service.submit_allocations(
            {"节点标识": node["节点编号"], "策略编号": "BAND-9401,BAND-9402,BAND-9403", "at": "2026-11-10T10:05:00"}
        )
        self.assertIsNotNone(entry, message)
        self.assertEqual(entry["执行结果条数"], 1)
        self.assertEqual(len(entry["逐条结果"]), 3)
        quotas = {item["策略编号"]: item["唯一配额"] for item in entry["逐条结果"]}
        self.assertEqual(quotas["BAND-9401"], "下载 300Mbps，突发 0Mbps，粒度 1 秒")
        self.assertEqual(quotas["BAND-9402"], "下载 100Mbps，突发 0Mbps，粒度 1 秒")
        self.assertEqual(quotas["BAND-9403"], "下载 50Mbps，突发 0Mbps，粒度 1 秒")
        self.assertEqual(len(store.executions()), 1)
        self.assertIn("300Mbps", store.executions()[0]["唯一配额"])

    def test_download_task_reports_traffic_and_worker_manifest(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9501",
            节点标识=node["节点编号"],
            下载带宽上限="100Mbps",
            突发额度="10Mbps",
            生效时段="长期",
        )
        self.approve_and_enable(policy, "2026-11-10T10:00:00")

        result, message = bandwidth_service.report_traffic(
            node["节点编号"],
            {"下载任务编号": "TASK-001", "实际流量MB": 200, "用时秒": 10, "at": "2026-11-10T10:10:00"},
        )
        self.assertIsNotNone(result, message)
        self.assertEqual(result["配额Mbps"], 100.0)
        self.assertTrue(result["是否超限"])
        self.assertEqual(len(store.traffic()), 1)
        self.assertEqual(store.executions()[0]["下载任务数"], 1)
        self.assertEqual(store.executions()[0]["实际下载流量MB"], 200.0)

        in_quota, message = bandwidth_service.report_traffic(
            node["节点编号"],
            {"下载任务编号": "TASK-002", "实际流量MB": 50, "用时秒": 10, "at": "2026-11-10T10:11:00"},
        )
        self.assertIsNotNone(in_quota, message)
        self.assertFalse(in_quota["是否超限"])

        worker_row = next(row for row in store.rows("worker") if row["节点编号"] == node["节点编号"])
        self.assertEqual(worker_row["带宽分配编号"], store.allocations()[0]["分配编号"])
        self.assertEqual(worker_row["带宽策略编号"], "BAND-9501")
        self.assertEqual(worker_row["唯一配额"], "下载 100Mbps，突发 10Mbps，粒度 1 秒")
        self.assertEqual(worker_row["限速执行编号"], store.executions()[0]["执行编号"])
        self.assertEqual(worker_row["带宽实际流量MB"], 250.0)
        self.assertNotIn("第二条限额", worker_row)

    def test_download_without_allocation_is_rejected(self) -> None:
        result, message = bandwidth_service.report_traffic(
            "WORK-UNKNOWN", {"下载任务编号": "TASK-X", "实际流量MB": 1}
        )
        self.assertIsNone(result)
        self.assertIn("没有可用的带宽分配", message)

    def test_worker_side_reads_allocation(self) -> None:
        node = self.add_node()
        policy = self.make_policy(
            策略编号="BAND-9601", 节点标识=node["节点编号"], 下载带宽上限="80Mbps", 生效时段="长期"
        )
        self.approve_and_enable(policy, "2026-11-10T10:00:00")
        worker_service = WorkerService()
        rows, total = worker_service.list_entries(size=50)
        self.assertEqual(total, len(store.rows("worker")))
        listed = next(row for row in rows if row["节点编号"] == node["节点编号"])
        self.assertEqual(listed["带宽分配编号"], store.allocations()[0]["分配编号"])
        self.assertEqual(listed["唯一配额"], "下载 80Mbps，突发 0Mbps，粒度 1 秒")

        entry, message = worker_service.run_action(node["id"], "标记离线")
        self.assertIsNotNone(entry, message)
        node_executions = [item for item in store.executions() if item["节点标识"] == node["节点编号"]]
        self.assertEqual(len(node_executions), 1)
        self.assertEqual(node_executions[0]["节点状态"], "离线")


if __name__ == "__main__":
    unittest.main()
