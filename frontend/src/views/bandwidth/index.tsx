import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'

import { request } from '@/api/client'

const ENDPOINT = '/api/bandwidth'
const columns = [
  "策略编号",
  "节点标识",
  "下载带宽上限",
  "上传带宽上限",
  "突发额度",
  "限速粒度秒",
  "优先级权重",
  "生效时段",
  "策略版本",
  "唯一配额"
]
const actions = [
  "提交策略评审",
  "启用策略",
  "暂停策略"
]
const statuses = [
  "草稿",
  "策略评审中",
  "已启用",
  "已暂停"
]
// 只有评审中的策略可以启用；已暂停的必须先重新评审。
const actionRules: Record<string, string[]> = {
  "提交策略评审": ["草稿", "已完成", "已暂停", "策略评审中"],
  "启用策略": ["策略评审中", "已启用"],
  "暂停策略": ["已启用", "已暂停"],
}
const datasets = [
  { key: 'policies', label: '带宽策略' },
  { key: 'allocations', label: '节点带宽分配' },
  { key: 'executions', label: '限速执行结果' },
  { key: 'history', label: '执行留档' },
  { key: 'traffic', label: '流量回写' },
]
const allocationColumns = [
  "分配编号", "节点标识", "策略编号", "策略版本", "版本号", "状态", "唯一配额", "生成时间", "数据来源"
]
const executionColumns = [
  "执行编号", "节点标识", "策略编号", "分配编号", "唯一配额", "执行状态", "节点状态", "切换次数", "实际下载流量MB"
]
const historyColumns = [
  "留档编号", "节点标识", "事件", "事件说明", "策略编号", "分配编号", "唯一配额", "节点状态", "留档时间"
]
const trafficColumns = [
  "流量编号", "节点标识", "下载任务编号", "实际流量MB", "实际速率Mbps", "配额Mbps", "是否超限", "执行编号", "回写时间"
]

type Row = Record<string, string | number | boolean | null>
type DatasetKey = 'policies' | 'allocations' | 'executions' | 'history' | 'traffic'

export default function BandwidthView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [dataset, setDataset] = useState<DatasetKey>('policies')
  const [datasetRows, setDatasetRows] = useState<Row[]>([])
  const [datasetTotal, setDatasetTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [toggle, setToggle] = useState(false)
  const [batch, setBatch] = useState({ node: '', policyIds: '', at: '' })
  const [batchResult, setBatchResult] = useState<Row[]>([])
  const filterFields = useMemo(() => columns.slice(0, 2), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('带宽策略列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽控制列表读取失败')
    }
  }, [filters])

  const reloadDataset = useCallback(async () => {
    if (dataset === 'policies') {
      setDatasetRows([])
      setDatasetTotal(0)
      return
    }
    const params = new URLSearchParams()
    if (filters["节点标识"]) {
      params.set('node', filters["节点标识"])
    }
    try {
      const response = await request(`${ENDPOINT}/${dataset}?${params.toString()}`)
      if (!response.ok) {
        throw new Error('带宽数据集读取失败')
      }
      const payload = await response.json()
      setDatasetRows(payload.items ?? [])
      setDatasetTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽数据集读取失败')
    }
  }, [dataset, filters])

  useEffect(() => {
    void reload()
  }, [reload, toggle])

  useEffect(() => {
    void reloadDataset()
  }, [reloadDataset, toggle])

  const stats = useMemo(() => {
    const enabled = rows.filter((row) => row.status === '已启用').length
    const reviewing = rows.filter((row) => row.status === '策略评审中').length
    return [
      { label: '带宽策略总数', value: total },
      { label: '已启用策略', value: enabled },
      { label: '评审中策略', value: reviewing },
    ]
  }, [rows, total])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message ?? payload.detail ?? '带宽控制动作未生效，请稍后重试')
      }
      setNotice(payload.message ?? '')
      setToggle((value) => !value)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽控制操作失败')
    }
  }

  async function reconcileNow() {
    setErrorMessage('')
    setNotice('')
    try {
      const values: Record<string, string> = {}
      if (filters["节点标识"]) {
        values["节点标识"] = filters["节点标识"]
      }
      const response = await request(`${ENDPOINT}/reconcile`, {
        method: 'POST',
        body: JSON.stringify({ values }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message ?? '带宽配额校准失败')
      }
      setNotice(payload.message ?? '带宽配额已校准')
      setToggle((value) => !value)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽配额校准失败')
    }
  }

  async function submitBatch(event: React.FormEvent) {
    event.preventDefault()
    setErrorMessage('')
    setNotice('')
    setBatchResult([])
    try {
      const values: Record<string, unknown> = { 节点标识: batch.node }
      if (batch.policyIds.trim()) {
        values["策略编号"] = batch.policyIds
      }
      if (batch.at) {
        values["at"] = batch.at
      }
      const response = await request(`${ENDPOINT}/batch`, {
        method: 'POST',
        body: JSON.stringify({ values }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message ?? payload.detail ?? '整组提交失败')
      }
      setNotice(payload.message ?? '整组提交完成')
      setBatchResult((payload.entry?.逐条结果 ?? []) as Row[])
      setToggle((value) => !value)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '整组提交失败')
    }
  }

  const activeColumns =
    dataset === 'allocations' ? allocationColumns
      : dataset === 'executions' ? executionColumns
        : dataset === 'history' ? historyColumns
          : dataset === 'traffic' ? trafficColumns
            : columns

  const displayedRows = dataset === 'policies' ? rows : datasetRows
  const displayedTotal = dataset === 'policies' ? total : datasetTotal

  return (
    <section className="page" data-module="bandwidth">
      <header className="page-head">
        <div>
          <h2>带宽控制管理</h2>
          <p className="page-desc">管理各采集节点的带宽控制策略，防止网络资源过载；同一节点只保留一条限速执行结果。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void reconcileNow()}>
            按生效时段校准
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出带宽控制清单
          </button>
          <Link className="btn" to="/worker">查看采集节点清单</Link>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <div className="tab-row">
        {datasets.map((item) => (
          <button
            key={item.key}
            type="button"
            className={`tab ${dataset === item.key ? 'active' : ''}`}
            onClick={() => setDataset(item.key as DatasetKey)}
          >
            {item.label}
          </button>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); setToggle((value) => !value) }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      {dataset === 'policies' && (
        <form className="filter-bar batch-bar" onSubmit={submitBatch}>
          <label className="filter-item">
            <span>整组提交节点</span>
            <input
              value={batch.node}
              placeholder="节点标识，如 WORK-0003"
              onChange={(event) => setBatch((prev) => ({ ...prev, node: event.target.value }))}
            />
          </label>
          <label className="filter-item">
            <span>策略编号（逗号分隔）</span>
            <input
              value={batch.policyIds}
              placeholder="BAND-0003,BAND-0004"
              onChange={(event) => setBatch((prev) => ({ ...prev, policyIds: event.target.value }))}
            />
          </label>
          <label className="filter-item">
            <span>评估时间</span>
            <input
              value={batch.at}
              placeholder="2026-12-01T00:00:00"
              onChange={(event) => setBatch((prev) => ({ ...prev, at: event.target.value }))}
            />
          </label>
          <button className="btn" type="submit">整组提交并逐条配额</button>
        </form>
      )}

      <table className="data-table">
        <thead>
          <tr>
            {activeColumns.map((column) => <th key={column}>{column}</th>)}
            {dataset === 'policies' && <th>可执行动作</th>}
          </tr>
        </thead>
        <tbody>
          {displayedRows.map((row) => (
            <tr key={String(row.id ?? row["分配编号"] ?? row["执行编号"] ?? row["留档编号"] ?? row["流量编号"])}>
              {activeColumns.map((column) => <td key={column}>{displayValue(row[column])}</td>)}
              {dataset === 'policies' && (
                <td className="row-actions">
                  {actions.map((action) => {
                    const allowed = actionRules[action].includes(String(row.status ?? ''))
                    return (
                      <button
                        key={action}
                        className="link"
                        type="button"
                        disabled={!allowed}
                        title={allowed ? action : `策略状态「${row.status}」不允许${action}`}
                        onClick={() => void runAction(action, row)}
                      >
                        {action}
                      </button>
                    )
                  })}
                </td>
              )}
            </tr>
          ))}
          {displayedRows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={activeColumns.length + 1}>暂无数据</td>
            </tr>
          )}
        </tbody>
      </table>

      {batchResult.length > 0 && (
        <table className="data-table batch-result">
          <thead>
            <tr>
              <th>策略编号</th>
              <th>策略版本</th>
              <th>唯一配额</th>
              <th>分配编号</th>
              <th>分配状态</th>
              <th>结果说明</th>
            </tr>
          </thead>
          <tbody>
            {batchResult.map((item, index) => (
              <tr key={`${item["策略编号"] ?? index}`}>
                <td>{displayValue(item["策略编号"])}</td>
                <td>{displayValue(item["策略版本"])}</td>
                <td>{displayValue(item["唯一配额"])}</td>
                <td>{displayValue(item["分配编号"])}</td>
                <td>{displayValue(item["分配状态"])}</td>
                <td>{displayValue(item["说明"])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <footer className="page-foot">
        <span>共 {displayedTotal} 条{dataset === 'policies' ? '带宽策略' : '数据集'}记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

function displayValue(value: Row[string]): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'boolean') return value ? '是' : '否'
  return String(value)
}
