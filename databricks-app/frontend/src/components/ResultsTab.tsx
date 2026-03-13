import { useState, useEffect, useMemo } from 'react'
import ActionButton from './ActionButton'

interface StepMetric {
  step_name: string
  duration_seconds: number | null
  rows_processed: number | null
  bytes_processed: number | null
  status: string
}

interface RunMetrics {
  _source_file: string
  platform: string
  platform_type?: string
  load_type: string
  scale_factor: number
  batch_id?: number | null
  start_time_iso?: string
  start_time?: number
  total_duration_seconds: number | null
  steps: StepMetric[]
  summary?: {
    total_steps: number
    completed_steps: number
    failed_steps: number
    total_rows_processed: number
    total_bytes_processed: number
    throughput_rows_per_second: number
    throughput_mb_per_second: number
  }
  cluster_instance_type?: string
  cluster_worker_count?: number
  cluster_master_type?: string
  cluster_configuration?: {
    worker_node_type?: string
    driver_node_type?: string
    number_of_worker_nodes?: number
  }
  cost_breakdown?: {
    compute_usd?: number
    software_usd?: number
  }
  total_cost_usd?: number
  dbu_cost_usd?: number
  per_table_details?: Record<string, {
    duration_seconds: number
    row_count: number
    bytes_processed?: number
    throughput_rows_per_sec?: number
    throughput_mb_per_sec?: number
  }>
  databricks_compute_type?: string
}

type GroupKey = string // "batch|10" or "incremental|100|2"

function groupKey(m: RunMetrics): GroupKey {
  const parts = [m.load_type, String(m.scale_factor)]
  if (m.load_type === 'incremental' && m.batch_id) parts.push(String(m.batch_id))
  return parts.join('|')
}

function groupLabel(key: GroupKey): string {
  const [loadType, sf, batchId] = key.split('|')
  let label = `${loadType.charAt(0).toUpperCase() + loadType.slice(1)} - SF ${sf}`
  if (batchId) label += ` (Batch ${batchId})`
  return label
}

function formatDuration(secs: number | null | undefined): string {
  if (secs == null) return '--'
  if (secs < 60) return `${secs.toFixed(1)}s`
  const mins = Math.floor(secs / 60)
  const s = secs % 60
  return `${mins}m ${s.toFixed(0)}s`
}

function formatNumber(n: number | null | undefined): string {
  if (n == null) return '--'
  return n.toLocaleString()
}

function formatMB(bytes: number | null | undefined): string {
  if (bytes == null) return '--'
  const mb = bytes / (1024 * 1024)
  if (mb > 1024) return `${(mb / 1024).toFixed(1)} GB`
  return `${mb.toFixed(1)} MB`
}

function formatCost(usd: number | null | undefined): string {
  if (usd == null) return '--'
  return `$${usd.toFixed(2)}`
}

function platformLabel(m: RunMetrics): string {
  const p = m.platform_type || m.platform || 'unknown'
  if (p === 'databricks') {
    const ct = m.databricks_compute_type
    return ct ? `Databricks (${ct})` : 'Databricks'
  }
  if (p === 'dataproc_serverless') return 'Dataproc Serverless'
  if (p === 'dataproc') return 'Dataproc'
  return p.charAt(0).toUpperCase() + p.slice(1)
}

function platformColor(m: RunMetrics): string {
  const p = m.platform_type || m.platform || ''
  if (p.includes('databricks')) return 'orange'
  if (p.includes('dataproc')) return 'blue'
  return 'slate'
}

const colorMap: Record<string, { bg: string; text: string; border: string; light: string }> = {
  orange: { bg: 'bg-orange-500', text: 'text-orange-700', border: 'border-orange-300', light: 'bg-orange-50' },
  blue: { bg: 'bg-blue-600', text: 'text-blue-700', border: 'border-blue-300', light: 'bg-blue-50' },
  slate: { bg: 'bg-slate-500', text: 'text-slate-700', border: 'border-slate-300', light: 'bg-slate-50' },
}

// ── Comparison card for a single (load_type, sf) group ────────────────────

function ComparisonGroup({ groupKey: gk, runs }: { groupKey: GroupKey; runs: RunMetrics[] }) {
  const [expanded, setExpanded] = useState(false)

  // Separate by platform and take latest per platform
  const byPlatform = useMemo(() => {
    const map: Record<string, RunMetrics[]> = {}
    for (const r of runs) {
      const key = r.platform_type || r.platform || 'unknown'
      if (!map[key]) map[key] = []
      map[key].push(r)
    }
    // Sort each group by start_time desc, keep latest
    for (const k of Object.keys(map)) {
      map[k].sort((a, b) => (b.start_time || 0) - (a.start_time || 0))
    }
    return map
  }, [runs])

  const platforms = Object.keys(byPlatform)
  const latestRuns = platforms.map(p => byPlatform[p][0])

  // Find winner (lowest duration)
  const durations = latestRuns.map(r => r.total_duration_seconds)
  const validDurations = durations.filter((d): d is number => d != null && d > 0)
  const minDuration = validDurations.length > 0 ? Math.min(...validDurations) : null

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm mb-6">
      {/* Header */}
      <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-800">{groupLabel(gk)}</h3>
        <span className="text-xs text-slate-400">{runs.length} run{runs.length !== 1 ? 's' : ''}</span>
      </div>

      {/* Side-by-side comparison */}
      <div className={`grid gap-0 divide-x divide-slate-100`} style={{ gridTemplateColumns: `repeat(${latestRuns.length}, 1fr)` }}>
        {latestRuns.map((run, i) => {
          const color = platformColor(run)
          const c = colorMap[color]
          const isWinner = run.total_duration_seconds === minDuration && latestRuns.length > 1
          return (
            <div key={i} className="p-4">
              {/* Platform badge */}
              <div className="flex items-center gap-2 mb-3">
                <span className={`inline-block px-2.5 py-1 text-xs font-semibold text-white rounded ${c.bg}`}>
                  {platformLabel(run)}
                </span>
                {isWinner && (
                  <span className="inline-block px-2 py-0.5 text-xs font-medium bg-green-100 text-green-700 rounded">
                    Faster
                  </span>
                )}
              </div>

              {/* Key metrics */}
              <div className="space-y-2.5">
                <MetricRow label="Total Duration" value={formatDuration(run.total_duration_seconds)} highlight={isWinner} />
                <MetricRow label="Total Cost" value={formatCost(run.total_cost_usd)} />
                <MetricRow label="Rows Processed" value={formatNumber(run.summary?.total_rows_processed)} />
                <MetricRow label="Data Size" value={formatMB(run.summary?.total_bytes_processed)} />
                <MetricRow label="Throughput" value={run.summary?.throughput_rows_per_second ? `${formatNumber(Math.round(run.summary.throughput_rows_per_second))} rows/s` : '--'} />
                <MetricRow label="Data Throughput" value={run.summary?.throughput_mb_per_second ? `${run.summary.throughput_mb_per_second.toFixed(1)} MB/s` : '--'} />

                <div className="border-t border-slate-100 pt-2 mt-2">
                  <div className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-1">Cluster</div>
                  <div className="text-xs text-slate-600">
                    {run.cluster_configuration?.driver_node_type || run.cluster_master_type || '--'} (driver)
                  </div>
                  <div className="text-xs text-slate-600">
                    {run.cluster_configuration?.worker_node_type || run.cluster_instance_type || '--'} x {run.cluster_configuration?.number_of_worker_nodes ?? run.cluster_worker_count ?? '--'} (workers)
                  </div>
                </div>

                {run.cost_breakdown && (
                  <div className="border-t border-slate-100 pt-2 mt-2">
                    <div className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-1">Cost Breakdown</div>
                    <div className="text-xs text-slate-600">Compute: {formatCost(run.cost_breakdown.compute_usd)}</div>
                    <div className="text-xs text-slate-600">Software/DBU: {formatCost(run.cost_breakdown.software_usd || run.dbu_cost_usd)}</div>
                  </div>
                )}

                <div className="text-xs text-slate-400 mt-2">
                  {run.start_time_iso ? new Date(run.start_time_iso).toLocaleString() : '--'}
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* ETL Step comparison (expandable) */}
      <div className="border-t border-slate-100">
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full flex items-center justify-between px-5 py-3 text-sm text-slate-600 hover:bg-slate-50 transition-colors"
        >
          <span className="font-medium">ETL Step Breakdown</span>
          <svg
            className={`w-4 h-4 text-slate-400 transition-transform ${expanded ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {expanded && (
          <div className="px-5 pb-4">
            {/* Steps table */}
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-slate-400 uppercase tracking-wide">
                  <th className="pb-2 pr-4">Step</th>
                  {latestRuns.map((run, i) => (
                    <th key={i} className="pb-2 pr-4">{platformLabel(run)}</th>
                  ))}
                  {latestRuns.length > 1 && <th className="pb-2">Diff</th>}
                </tr>
              </thead>
              <tbody className="text-slate-700">
                {getComparableSteps(latestRuns).map((stepName) => {
                  const stepValues = latestRuns.map(r => {
                    const step = r.steps?.find(s => s.step_name === stepName)
                    return step?.duration_seconds
                  })
                  const validVals = stepValues.filter((v): v is number => v != null)
                  const min = validVals.length > 0 ? Math.min(...validVals) : null
                  const max = validVals.length > 0 ? Math.max(...validVals) : null

                  return (
                    <tr key={stepName} className="border-t border-slate-50">
                      <td className="py-1.5 pr-4 font-medium">{stepName.replace(/_/g, ' ')}</td>
                      {stepValues.map((v, i) => (
                        <td key={i} className={`py-1.5 pr-4 ${v === min && latestRuns.length > 1 ? 'text-green-600 font-semibold' : ''}`}>
                          {formatDuration(v)}
                        </td>
                      ))}
                      {latestRuns.length > 1 && (
                        <td className="py-1.5">
                          {min != null && max != null && max > 0
                            ? <span className={max > min * 1.1 ? 'text-amber-600' : 'text-slate-400'}>
                                {((max - min) / max * 100).toFixed(0)}%
                              </span>
                            : '--'
                          }
                        </td>
                      )}
                    </tr>
                  )
                })}
              </tbody>
            </table>

            {/* Per-table details if available */}
            {latestRuns.some(r => r.per_table_details && Object.keys(r.per_table_details).length > 0) && (
              <div className="mt-4">
                <div className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-2">Per-Table Timing (top 10 by duration)</div>
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left text-slate-400 uppercase tracking-wide">
                      <th className="pb-2 pr-4">Table</th>
                      {latestRuns.map((run, i) => (
                        <th key={i} className="pb-2 pr-4">{platformLabel(run)}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="text-slate-700">
                    {getTopTables(latestRuns, 10).map(table => (
                      <tr key={table} className="border-t border-slate-50">
                        <td className="py-1 pr-4 font-medium">{table}</td>
                        {latestRuns.map((run, i) => {
                          const td = run.per_table_details?.[table]
                          return (
                            <td key={i} className="py-1 pr-4">
                              {td ? `${formatDuration(td.duration_seconds)} (${formatNumber(td.row_count)} rows)` : '--'}
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Duration bar chart */}
            {latestRuns.length > 1 && (
              <div className="mt-4">
                <div className="text-xs font-medium text-slate-400 uppercase tracking-wide mb-2">Duration Comparison</div>
                <DurationBars runs={latestRuns} />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function MetricRow({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="flex justify-between items-baseline">
      <span className="text-xs text-slate-500">{label}</span>
      <span className={`text-sm font-mono ${highlight ? 'text-green-600 font-bold' : 'text-slate-800'}`}>{value}</span>
    </div>
  )
}

function getComparableSteps(runs: RunMetrics[]): string[] {
  const allSteps = new Set<string>()
  for (const r of runs) {
    for (const s of r.steps || []) {
      allSteps.add(s.step_name)
    }
  }
  // Order: spark_session, database/schema, bronze, silver, gold, dq
  const order = ['spark_session', 'database', 'schema', 'bronze', 'silver', 'gold', 'dq']
  return Array.from(allSteps).sort((a, b) => {
    const ai = order.findIndex(o => a.toLowerCase().includes(o))
    const bi = order.findIndex(o => b.toLowerCase().includes(o))
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi)
  })
}

function getTopTables(runs: RunMetrics[], limit: number): string[] {
  const allTables = new Map<string, number>()
  for (const r of runs) {
    for (const [table, detail] of Object.entries(r.per_table_details || {})) {
      const cur = allTables.get(table) || 0
      allTables.set(table, Math.max(cur, detail.duration_seconds || 0))
    }
  }
  return Array.from(allTables.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([table]) => table)
}

function DurationBars({ runs }: { runs: RunMetrics[] }) {
  const maxDur = Math.max(...runs.map(r => r.total_duration_seconds || 0))
  if (maxDur === 0) return null

  return (
    <div className="space-y-2">
      {runs.map((run, i) => {
        const dur = run.total_duration_seconds || 0
        const pct = maxDur > 0 ? (dur / maxDur) * 100 : 0
        const color = platformColor(run)
        const c = colorMap[color]
        return (
          <div key={i} className="flex items-center gap-3">
            <span className="text-xs text-slate-600 w-32 shrink-0">{platformLabel(run)}</span>
            <div className="flex-1 bg-slate-100 rounded-full h-5 overflow-hidden">
              <div
                className={`h-full ${c.bg} rounded-full flex items-center justify-end pr-2 transition-all`}
                style={{ width: `${Math.max(pct, 8)}%` }}
              >
                <span className="text-[10px] text-white font-semibold">{formatDuration(dur)}</span>
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── All Runs Table ─────────────────────────────────────────────────────────

function AllRunsTable({ metrics }: { metrics: RunMetrics[] }) {
  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden">
      <div className="px-5 py-3 border-b border-slate-100">
        <h3 className="text-sm font-semibold text-slate-700">All Runs ({metrics.length})</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-slate-50 text-left text-slate-500 uppercase tracking-wide">
              <th className="px-4 py-2">Platform</th>
              <th className="px-4 py-2">Load Type</th>
              <th className="px-4 py-2">SF</th>
              <th className="px-4 py-2">Batch</th>
              <th className="px-4 py-2">Duration</th>
              <th className="px-4 py-2">Rows</th>
              <th className="px-4 py-2">Throughput</th>
              <th className="px-4 py-2">Cost</th>
              <th className="px-4 py-2">Cluster</th>
              <th className="px-4 py-2">Date</th>
            </tr>
          </thead>
          <tbody>
            {metrics.map((m, i) => {
              const color = platformColor(m)
              const c = colorMap[color]
              return (
                <tr key={i} className="border-t border-slate-50 hover:bg-slate-50">
                  <td className="px-4 py-2">
                    <span className={`inline-block px-2 py-0.5 text-[10px] font-semibold text-white rounded ${c.bg}`}>
                      {platformLabel(m)}
                    </span>
                  </td>
                  <td className="px-4 py-2 capitalize">{m.load_type}</td>
                  <td className="px-4 py-2">{m.scale_factor}</td>
                  <td className="px-4 py-2">{m.batch_id ?? '--'}</td>
                  <td className="px-4 py-2 font-mono font-medium">{formatDuration(m.total_duration_seconds)}</td>
                  <td className="px-4 py-2">{formatNumber(m.summary?.total_rows_processed)}</td>
                  <td className="px-4 py-2">{m.summary?.throughput_rows_per_second ? `${Math.round(m.summary.throughput_rows_per_second).toLocaleString()} r/s` : '--'}</td>
                  <td className="px-4 py-2">{formatCost(m.total_cost_usd)}</td>
                  <td className="px-4 py-2 text-slate-500">
                    {m.cluster_configuration?.worker_node_type || m.cluster_instance_type || '--'} x{m.cluster_configuration?.number_of_worker_nodes ?? m.cluster_worker_count ?? '?'}
                  </td>
                  <td className="px-4 py-2 text-slate-400">
                    {m.start_time_iso ? new Date(m.start_time_iso).toLocaleDateString() : '--'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Main Component ────────────────────────────────────────────────────────

export default function ResultsTab() {
  const [metricsPath, setMetricsPath] = useState('gs://sumit_prakash_gcs/tpcdi/metrics')
  const [metrics, setMetrics] = useState<RunMetrics[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<'comparison' | 'all'>('comparison')
  const [filterPlatform, setFilterPlatform] = useState<string>('all')
  const [filterLoadType, setFilterLoadType] = useState<string>('all')

  const loadMetrics = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`/api/metrics/list?metrics_path=${encodeURIComponent(metricsPath)}`)
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Failed to load metrics')
      if (data.error) setError(data.error)
      setMetrics(data.metrics || [])
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadMetrics() }, [])

  // Filters
  const filteredMetrics = useMemo(() => {
    return metrics.filter(m => {
      if (filterPlatform !== 'all' && (m.platform_type || m.platform) !== filterPlatform) return false
      if (filterLoadType !== 'all' && m.load_type !== filterLoadType) return false
      return true
    })
  }, [metrics, filterPlatform, filterLoadType])

  // Group by (load_type, sf, batch_id)
  const groups = useMemo(() => {
    const map = new Map<GroupKey, RunMetrics[]>()
    for (const m of filteredMetrics) {
      const key = groupKey(m)
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(m)
    }
    // Sort groups by SF ascending
    return Array.from(map.entries()).sort((a, b) => {
      const sfA = parseInt(a[0].split('|')[1])
      const sfB = parseInt(b[0].split('|')[1])
      return sfA - sfB
    })
  }, [filteredMetrics])

  // Unique platforms and load types for filters
  const platforms = useMemo(() => {
    const set = new Set(metrics.map(m => m.platform_type || m.platform))
    return Array.from(set)
  }, [metrics])
  const loadTypes = useMemo(() => {
    const set = new Set(metrics.map(m => m.load_type))
    return Array.from(set)
  }, [metrics])

  return (
    <div>
      <h3 className="text-lg font-semibold text-slate-800 mb-1">Benchmark Results</h3>
      <p className="text-sm text-slate-500 mb-4">
        Compare Databricks vs Dataproc benchmark runs side by side, grouped by load type and scale factor.
      </p>

      {/* Metrics path & load */}
      <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4 mb-4">
        <div className="flex gap-3 items-end">
          <div className="flex-1">
            <label className="block text-xs font-medium text-slate-600 mb-1">Metrics Path</label>
            <input
              type="text"
              value={metricsPath}
              onChange={e => setMetricsPath(e.target.value)}
              className="w-full border border-slate-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent"
              placeholder="gs://bucket/tpcdi/metrics"
            />
          </div>
          <ActionButton label={loading ? 'Loading...' : 'Load Results'} onClick={loadMetrics} loading={loading} variant="primary" />
        </div>
        {error && <p className="text-xs text-red-500 mt-2">{error}</p>}
      </div>

      {metrics.length > 0 && (
        <>
          {/* Filters & view toggle */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex gap-3">
              <select
                value={filterPlatform}
                onChange={e => setFilterPlatform(e.target.value)}
                className="border border-slate-300 rounded-md px-3 py-1.5 text-sm bg-white"
              >
                <option value="all">All Platforms</option>
                {platforms.map(p => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
              <select
                value={filterLoadType}
                onChange={e => setFilterLoadType(e.target.value)}
                className="border border-slate-300 rounded-md px-3 py-1.5 text-sm bg-white"
              >
                <option value="all">All Load Types</option>
                {loadTypes.map(lt => (
                  <option key={lt} value={lt}>{lt}</option>
                ))}
              </select>
            </div>
            <div className="flex bg-slate-100 rounded-md overflow-hidden">
              <button
                onClick={() => setViewMode('comparison')}
                className={`px-3 py-1.5 text-xs font-medium ${viewMode === 'comparison' ? 'bg-purple-600 text-white' : 'text-slate-600 hover:bg-slate-200'}`}
              >
                Comparison
              </button>
              <button
                onClick={() => setViewMode('all')}
                className={`px-3 py-1.5 text-xs font-medium ${viewMode === 'all' ? 'bg-purple-600 text-white' : 'text-slate-600 hover:bg-slate-200'}`}
              >
                All Runs
              </button>
            </div>
          </div>

          {/* Summary cards */}
          <div className="grid grid-cols-4 gap-3 mb-5">
            <SummaryCard label="Total Runs" value={String(filteredMetrics.length)} />
            <SummaryCard label="Platforms" value={String(platforms.length)} />
            <SummaryCard label="Scale Factors" value={String(new Set(filteredMetrics.map(m => m.scale_factor)).size)} />
            <SummaryCard
              label="Avg Duration"
              value={formatDuration(
                filteredMetrics.reduce((sum, m) => sum + (m.total_duration_seconds || 0), 0) / (filteredMetrics.length || 1)
              )}
            />
          </div>

          {/* Main content */}
          {viewMode === 'comparison' ? (
            groups.length === 0 ? (
              <p className="text-sm text-slate-400 text-center py-8">No matching runs found.</p>
            ) : (
              groups.map(([key, runs]) => (
                <ComparisonGroup key={key} groupKey={key} runs={runs} />
              ))
            )
          ) : (
            <AllRunsTable metrics={filteredMetrics} />
          )}
        </>
      )}

      {!loading && metrics.length === 0 && !error && (
        <div className="text-center py-12 text-slate-400">
          <p className="text-lg mb-1">No benchmark results found</p>
          <p className="text-sm">Run benchmarks on Databricks and Dataproc, then load results from the metrics path.</p>
        </div>
      )}
    </div>
  )
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm px-4 py-3">
      <div className="text-xs text-slate-400 uppercase tracking-wide">{label}</div>
      <div className="text-xl font-bold text-slate-800 mt-0.5">{value}</div>
    </div>
  )
}
