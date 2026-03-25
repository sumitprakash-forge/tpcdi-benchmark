import { useState, useEffect, Fragment } from 'react'

interface RunHistoryStep {
  name: string
  status: string
  output?: string
  duration_ms?: number
  link?: string
}

interface RunHistoryCost {
  vm_cost_usd?: number
  dataproc_premium_usd?: number
  ssd_cost_usd?: number
  total_cost_usd?: number
  dcu_hours?: number
  duration_ms?: number
  duration_hours?: number
  cluster?: {
    master_type?: string
    worker_type?: string
    worker_node_type?: string
    driver_node_type?: string
    driver_type?: string
    num_workers?: number
    ssd_count?: number
    local_ssd_count?: number
  }
  [key: string]: any
}

interface RunHistoryEntry {
  platform: string
  mode: string
  flow_type?: string
  scale_factor: string | number
  steps: RunHistoryStep[]
  costs: (null | RunHistoryCost)[]
  completed_at: string
  saved_at: string
  user_email?: string
  run_by?: string
  run_by_email?: string
  _filename: string
  status?: string
  compute_mode?: string
  serverless_tier?: string
  run_id?: string
  batch_run_id?: string
  b2_run_id?: string
  b3_run_id?: string
  timestamp?: string
  total_duration_ms?: number
}

function formatDurationMs(ms: number | undefined | null): string {
  if (ms == null) return '--'
  const secs = ms / 1000
  if (secs < 60) return `${secs.toFixed(1)}s`
  const mins = Math.floor(secs / 60)
  const s = secs % 60
  if (mins < 60) return `${mins}m ${s.toFixed(0)}s`
  const hrs = Math.floor(mins / 60)
  const rm = mins % 60
  return `${hrs}h ${rm}m`
}

/** Parse duration breakdown from step output string.
 * Dataproc: "Cluster: Xm Ys" and "Job: Xm Ys"
 * Databricks: "Setup: Xm Ys" and "Exec: Xm Ys"  */
function parseDurationBreakdown(output: string | undefined): { setup: string; exec: string } | null {
  if (!output) return null
  // Dataproc format
  const clusterMatch = output.match(/Cluster:\s*([^|,]+)/)
  const jobMatch = output.match(/Job:\s*([^|,]+)/)
  if (clusterMatch || jobMatch) {
    return {
      setup: clusterMatch ? clusterMatch[1].trim() : '--',
      exec: jobMatch ? jobMatch[1].trim() : '--',
    }
  }
  // Databricks format
  const setupMatch = output.match(/Setup:\s*([^|,]+)/)
  const execMatch = output.match(/Exec:\s*([^|,]+)/)
  if (setupMatch || execMatch) {
    return {
      setup: setupMatch ? setupMatch[1].trim() : '--',
      exec: execMatch ? execMatch[1].trim() : '--',
    }
  }
  return null
}

/**
 * Map step index to cost index.
 * Steps array: [0: prereq, 1: batch, 2: inc-b2, 3: inc-b3]
 * Costs array: [0: batch, 1: inc-b2, 2: inc-b3]
 * So cost for step si is costs[si - 1] for si >= 1, null for si === 0.
 */
function getCostForStep(entry: RunHistoryEntry, stepIndex: number): RunHistoryCost | null {
  if (!entry.costs) return null
  // Prereq/check steps have no cost
  const stepName = (entry.steps?.[stepIndex]?.name || '').toLowerCase()
  if (stepName.includes('prereq') || stepName.includes('check') || stepName.includes('gcloud') || stepName.includes('network')) {
    return null
  }
  // Count how many non-prereq steps came before this one to find the cost index
  let costIdx = 0
  for (let i = 0; i < stepIndex; i++) {
    const name = (entry.steps?.[i]?.name || '').toLowerCase()
    if (!name.includes('prereq') && !name.includes('check') && !name.includes('gcloud') && !name.includes('network')) {
      costIdx++
    }
  }
  return entry.costs[costIdx] || null
}

interface RunHistorySectionProps {
  platform: 'databricks' | 'dataproc'
}

export default function RunHistorySection({ platform }: RunHistorySectionProps) {
  const [entries, setEntries] = useState<RunHistoryEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expandedRow, setExpandedRow] = useState<string | null>(null)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [fixing, setFixing] = useState<string | null>(null)

  const fetchHistory = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch('/api/databricks/run-history')
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Failed to load run history')
      const all: RunHistoryEntry[] = Array.isArray(data) ? data : data.entries || []
      // Filter by platform
      const filtered = all.filter(e => (e.platform || '').toLowerCase() === platform)
      setEntries(filtered)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchHistory() }, [platform])

  const handleFixStatus = async (entry: RunHistoryEntry) => {
    setFixing(entry._filename)
    try {
      // Mark all non-prereq steps that show 'running' as 'success' since pipeline completed
      const fixedSteps = (entry.steps || []).map(s => {
        if (s.status === 'running') return { ...s, status: 'success' }
        return s
      })
      const allSuccess = fixedSteps.every(s => s.status === 'success')
      const res = await fetch(`/api/databricks/run-history/${encodeURIComponent(entry._filename)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ updates: { status: allSuccess ? 'completed' : 'failed', steps: fixedSteps } }),
      })
      if (!res.ok) throw new Error('Fix failed')
      await fetchHistory()
    } catch (e: any) {
      setError(e.message)
    } finally {
      setFixing(null)
    }
  }

  const handleDelete = async (filename: string) => {
    if (!confirm('Delete this run history entry?')) return
    setDeleting(filename)
    try {
      const res = await fetch(`/api/databricks/run-history/${encodeURIComponent(filename)}`, { method: 'DELETE' })
      if (!res.ok) {
        const data = await res.json()
        throw new Error(data.detail || 'Delete failed')
      }
      await fetchHistory()
    } catch (e: any) {
      setError(e.message)
    } finally {
      setDeleting(null)
    }
  }

  const totalCostForEntry = (entry: RunHistoryEntry): number | null => {
    if (!entry.costs || entry.costs.length === 0) return null
    let sum = 0
    let hasAny = false
    for (const c of entry.costs) {
      if (c && c.total_cost_usd != null) {
        sum += c.total_cost_usd
        hasAny = true
      }
    }
    return hasAny ? sum : null
  }

  const completedSteps = (entry: RunHistoryEntry): number => {
    if (!entry.steps) return 0
    return entry.steps.filter(s => s.status === 'success').length
  }

  const totalDurationForEntry = (entry: RunHistoryEntry): number | null => {
    if (entry.total_duration_ms) return entry.total_duration_ms
    if (!entry.steps || entry.steps.length === 0) return null
    let sum = 0
    let hasAny = false
    for (const s of entry.steps) {
      if (s.duration_ms != null) { sum += s.duration_ms; hasAny = true }
    }
    return hasAny ? sum : null
  }

  const jobDurationForEntry = (entry: RunHistoryEntry): number | null => {
    if (!entry.steps || entry.steps.length === 0) return null
    let sum = 0
    let hasAny = false
    for (const s of entry.steps) {
      // Skip prereq/check steps — only count actual job runs
      const name = (s.name || '').toLowerCase()
      if (name.includes('prereq') || name.includes('check') || name.includes('gcloud') || name.includes('network')) continue
      if (s.duration_ms != null) { sum += s.duration_ms; hasAny = true }
    }
    return hasAny ? sum : null
  }

  const statusDot = (status: string) => {
    if (status === 'success') return 'bg-green-400'
    if (status === 'error' || status === 'failed') return 'bg-red-400'
    if (status === 'skipped') return 'bg-slate-300'
    return 'bg-yellow-400'
  }

  const platformColor = platform === 'databricks' ? 'bg-orange-500' : 'bg-blue-600'
  const platformName = platform.charAt(0).toUpperCase() + platform.slice(1)

  return (
    <div className="mt-8">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-lg font-semibold text-slate-800">Run History</h3>
          <p className="text-sm text-slate-500">
            Past {platformName} E2E benchmark runs auto-saved from the orchestration flow.
          </p>
        </div>
        <button
          onClick={fetchHistory}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-purple-600 hover:text-purple-800 border border-purple-200 hover:border-purple-300 rounded-md bg-white hover:bg-purple-50 disabled:opacity-50 transition-colors"
        >
          <svg
            className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh
        </button>
      </div>

      {loading && (
        <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-8 text-center text-sm text-slate-400">
          Loading run history...
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-600 mb-4">
          {error}
        </div>
      )}

      {!loading && entries.length === 0 && !error && (
        <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-8 text-center text-slate-400">
          <p className="text-sm">No {platformName} run history found. Complete an E2E benchmark run to see entries here.</p>
        </div>
      )}

      {!loading && entries.length > 0 && (
        <div className="bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-slate-100 flex items-center justify-between">
            <h4 className="text-sm font-semibold text-slate-700">
              <span className={`inline-block px-2 py-0.5 text-[10px] font-semibold text-white rounded ${platformColor} mr-2`}>
                {platformName}
              </span>
              {entries.length} Run{entries.length !== 1 ? 's' : ''}
            </h4>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 text-left text-slate-500 uppercase tracking-wide">
                  <th className="px-4 py-2 w-8"></th>
                  <th className="px-4 py-2">Flow</th>
                  <th className="px-4 py-2">Compute</th>
                  <th className="px-4 py-2">Scale Factor</th>
                  <th className="px-4 py-2">Run By</th>
                  <th className="px-4 py-2">Status</th>
                  <th className="px-4 py-2">Steps</th>
                  <th className="px-4 py-2">Total Cost</th>
                  <th className="px-4 py-2">Total Duration</th>
                  <th className="px-4 py-2">Job Duration</th>
                  <th className="px-4 py-2">Saved</th>
                  <th className="px-4 py-2 w-16"></th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => {
                  const isExpanded = expandedRow === entry._filename
                  const cost = totalCostForEntry(entry)
                  const totalDur = totalDurationForEntry(entry)
                  const jobDur = jobDurationForEntry(entry)
                  const savedTime = entry.saved_at || entry.completed_at || entry.timestamp
                  return (
                    <Fragment key={entry._filename}>
                      <tr
                        className="border-t border-slate-50 hover:bg-slate-50 cursor-pointer"
                        onClick={() => setExpandedRow(isExpanded ? null : entry._filename)}
                      >
                        <td className="px-4 py-2 text-slate-400">
                          <svg
                            className={`w-3.5 h-3.5 transition-transform ${isExpanded ? 'rotate-90' : ''}`}
                            fill="none" viewBox="0 0 24 24" stroke="currentColor"
                          >
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                          </svg>
                        </td>
                        <td className="px-4 py-2 text-slate-700">
                          <span className={`inline-block px-2 py-0.5 text-[10px] font-semibold rounded ${
                            (entry.flow_type || entry.mode) === 'e2e' ? 'bg-purple-100 text-purple-700' : 'bg-slate-100 text-slate-600'
                          }`}>
                            {(entry.flow_type || entry.mode || 'e2e').toUpperCase()}
                          </span>
                        </td>
                        <td className="px-4 py-2 text-slate-700">
                          <span className={`inline-block px-2 py-0.5 text-[10px] font-semibold rounded ${
                            entry.compute_mode === 'serverless'
                              ? entry.serverless_tier === 'premium' ? 'bg-amber-100 text-amber-700' : 'bg-sky-100 text-sky-700'
                              : 'bg-slate-100 text-slate-600'
                          }`}>
                            {entry.compute_mode === 'serverless'
                              ? entry.serverless_tier === 'premium' ? 'Serverless Premium' : 'Serverless Std'
                              : 'Classic'}
                          </span>
                        </td>
                        <td className="px-4 py-2 text-slate-700">SF {entry.scale_factor}</td>
                        <td className="px-4 py-2 text-slate-600 truncate max-w-[140px]" title={entry.run_by_email || entry.run_by || entry.user_email || ''}>
                          {entry.run_by || entry.user_email || '--'}
                        </td>
                        <td className="px-4 py-2">
                          <span className={`flex items-center gap-1.5`}>
                            <span className={`w-2 h-2 rounded-full ${entry.status === 'completed' ? 'bg-green-400' : entry.status === 'failed' ? 'bg-red-400' : 'bg-yellow-400'}`} />
                            <span className="text-slate-700 capitalize">{entry.status || 'unknown'}</span>
                          </span>
                        </td>
                        <td className="px-4 py-2 text-slate-700">
                          {entry.steps ? `${completedSteps(entry)} / ${entry.steps.length}` : '--'}
                        </td>
                        <td className="px-4 py-2 font-mono font-medium text-slate-800">
                          {cost != null ? `$${cost.toFixed(2)}` : '--'}
                        </td>
                        <td className="px-4 py-2 font-mono text-slate-700">
                          {formatDurationMs(totalDur)}
                        </td>
                        <td className="px-4 py-2 font-mono text-slate-700">
                          {formatDurationMs(jobDur)}
                        </td>
                        <td className="px-4 py-2 text-slate-400">
                          {savedTime ? new Date(savedTime).toLocaleString() : '--'}
                        </td>
                        <td className="px-4 py-2 flex items-center gap-1">
                          {entry.status === 'failed' && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleFixStatus(entry) }}
                              disabled={fixing === entry._filename}
                              className="text-green-500 hover:text-green-700 disabled:opacity-50 p-1"
                              title="Mark running steps as success"
                            >
                              {fixing === entry._filename ? (
                                <svg className="w-4 h-4 animate-spin" viewBox="0 0 24 24" fill="none">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                                </svg>
                              ) : (
                                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                                </svg>
                              )}
                            </button>
                          )}
                          <button
                            onClick={(e) => { e.stopPropagation(); handleDelete(entry._filename) }}
                            disabled={deleting === entry._filename}
                            className="text-red-400 hover:text-red-600 disabled:opacity-50 p-1"
                            title="Delete run"
                          >
                            {deleting === entry._filename ? (
                              <svg className="w-4 h-4 animate-spin" viewBox="0 0 24 24" fill="none">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                              </svg>
                            ) : (
                              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                              </svg>
                            )}
                          </button>
                        </td>
                      </tr>

                      {isExpanded && entry.steps && (
                        <tr className="bg-slate-50">
                          <td colSpan={12} className="px-6 py-4">
                            <table className="w-full text-xs">
                              <thead>
                                <tr className="text-left text-slate-400 uppercase tracking-wide">
                                  <th className="pb-2 pr-4">Step</th>
                                  <th className="pb-2 pr-4">Status</th>
                                  <th className="pb-2 pr-4">Cluster Config</th>
                                  <th className="pb-2 pr-4">Duration (Setup + Exec = Total)</th>
                                  <th className="pb-2 pr-4">Cost (Compute + DBU + SSD = Total)</th>
                                  <th className="pb-2">Link</th>
                                </tr>
                              </thead>
                              <tbody className="text-slate-700">
                                {entry.steps.map((step, si) => {
                                  const stepCost = getCostForStep(entry, si)
                                  const durBreakdown = parseDurationBreakdown(step.output)
                                  const cluster = stepCost?.cluster
                                  return (
                                    <tr key={si} className="border-t border-slate-100">
                                      <td className="py-1.5 pr-4 font-medium">{step.name}</td>
                                      <td className="py-1.5 pr-4">
                                        <span className="flex items-center gap-1.5">
                                          <span className={`w-2 h-2 rounded-full ${statusDot(step.status)}`} />
                                          {step.status}
                                        </span>
                                      </td>
                                      <td className="py-1.5 pr-4 text-[10px] text-slate-500">
                                        {cluster ? (
                                          <div className="leading-tight">
                                            {platform === 'dataproc' ? (
                                              (cluster.master_type || '').startsWith('Serverless') ? (
                                                <div>{cluster.master_type}</div>
                                              ) : (
                                                <>
                                                  <div>M: {cluster.master_type || '--'}</div>
                                                  <div>W: {cluster.worker_type || '--'} x{cluster.num_workers ?? '--'}</div>
                                                  {(cluster.ssd_count != null && cluster.ssd_count > 0) && <div>SSD: {cluster.ssd_count}/node</div>}
                                                </>
                                              )
                                            ) : (
                                              <>
                                                <div>D: {cluster.driver_node_type || cluster.driver_type || '--'}</div>
                                                <div>W: {cluster.worker_node_type || cluster.worker_type || '--'} x{cluster.num_workers ?? '--'}</div>
                                                {(cluster.local_ssd_count != null && cluster.local_ssd_count > 0) && <div>SSD: {cluster.local_ssd_count}/node</div>}
                                              </>
                                            )}
                                          </div>
                                        ) : '--'}
                                      </td>
                                      <td className="py-1.5 pr-4 font-mono">
                                        {durBreakdown ? (
                                          <span>
                                            <span className="text-slate-500">{durBreakdown.setup}</span>
                                            {' + '}
                                            <span className="text-slate-500">{durBreakdown.exec}</span>
                                            {' = '}
                                            <span className="font-medium">{formatDurationMs(step.duration_ms)}</span>
                                          </span>
                                        ) : (
                                          formatDurationMs(step.duration_ms)
                                        )}
                                      </td>
                                      <td className="py-1.5 pr-4 font-mono">
                                        {stepCost?.total_cost_usd != null ? (
                                          (stepCost.cluster?.master_type || '').startsWith('Serverless') ? (
                                            <span>
                                              <span className="text-slate-500">DCU: ${stepCost.vm_cost_usd?.toFixed(2) ?? '0.00'}</span>
                                              {stepCost.dataproc_premium_usd && stepCost.dataproc_premium_usd > 0 ? (
                                                <>{' + '}<span className="text-slate-500">Shuffle: ${stepCost.dataproc_premium_usd.toFixed(2)}</span></>
                                              ) : null}
                                              {' = '}
                                              <span className="font-medium">${stepCost.total_cost_usd.toFixed(2)}</span>
                                              {stepCost.dcu_hours ? <span className="text-slate-400 ml-1">({stepCost.dcu_hours.toFixed(1)} DCU-hrs)</span> : null}
                                            </span>
                                          ) : (
                                            <span>
                                              <span className="text-slate-500">${stepCost.vm_cost_usd?.toFixed(2) ?? '0.00'}</span>
                                              {' + '}
                                              <span className="text-slate-500">${stepCost.dbu_cost_usd?.toFixed(2) ?? stepCost.dataproc_premium_usd?.toFixed(2) ?? '0.00'}</span>
                                              {' + '}
                                              <span className="text-slate-500">${stepCost.ssd_cost_usd?.toFixed(2) ?? '0.00'}</span>
                                              {' = '}
                                              <span className="font-medium">${stepCost.total_cost_usd.toFixed(2)}</span>
                                            </span>
                                          )
                                        ) : '--'}
                                      </td>
                                      <td className="py-1.5">
                                        {step.link ? (
                                          <a
                                            href={step.link}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-purple-600 hover:text-purple-800 underline"
                                            onClick={(e) => e.stopPropagation()}
                                          >
                                            View
                                          </a>
                                        ) : '--'}
                                      </td>
                                    </tr>
                                  )
                                })}
                              </tbody>
                            </table>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
