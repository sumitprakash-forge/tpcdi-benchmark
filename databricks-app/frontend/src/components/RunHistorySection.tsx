import { useState, useEffect, Fragment } from 'react'

interface RunHistoryStep {
  name: string
  status: string
  output?: string
  duration_ms?: number
  link?: string
}

interface RunHistoryEntry {
  platform: string
  mode: string
  scale_factor: string | number
  steps: RunHistoryStep[]
  costs: (null | { total_cost_usd?: number; [key: string]: any })[]
  completed_at: string
  saved_at: string
  user_email?: string
  _filename: string
  status?: string
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

interface RunHistorySectionProps {
  platform: 'databricks' | 'dataproc'
}

export default function RunHistorySection({ platform }: RunHistorySectionProps) {
  const [entries, setEntries] = useState<RunHistoryEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expandedRow, setExpandedRow] = useState<string | null>(null)
  const [deleting, setDeleting] = useState<string | null>(null)

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
                  <th className="px-4 py-2">Mode</th>
                  <th className="px-4 py-2">Scale Factor</th>
                  <th className="px-4 py-2">Status</th>
                  <th className="px-4 py-2">Steps</th>
                  <th className="px-4 py-2">Total Cost</th>
                  <th className="px-4 py-2">Saved</th>
                  <th className="px-4 py-2 w-16"></th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => {
                  const isExpanded = expandedRow === entry._filename
                  const cost = totalCostForEntry(entry)
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
                        <td className="px-4 py-2 capitalize text-slate-700">{entry.mode || 'e2e'}</td>
                        <td className="px-4 py-2 text-slate-700">SF {entry.scale_factor}</td>
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
                          {cost != null ? `$${cost.toFixed(2)}` : entry.total_duration_ms ? formatDurationMs(entry.total_duration_ms) : '--'}
                        </td>
                        <td className="px-4 py-2 text-slate-400">
                          {savedTime ? new Date(savedTime).toLocaleString() : '--'}
                        </td>
                        <td className="px-4 py-2">
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
                          <td colSpan={8} className="px-6 py-4">
                            <table className="w-full text-xs">
                              <thead>
                                <tr className="text-left text-slate-400 uppercase tracking-wide">
                                  <th className="pb-2 pr-4">Step</th>
                                  <th className="pb-2 pr-4">Status</th>
                                  <th className="pb-2 pr-4">Duration</th>
                                  <th className="pb-2 pr-4">Cost</th>
                                  <th className="pb-2">Link</th>
                                </tr>
                              </thead>
                              <tbody className="text-slate-700">
                                {entry.steps.map((step, si) => {
                                  const stepCost = entry.costs?.[si]
                                  return (
                                    <tr key={si} className="border-t border-slate-100">
                                      <td className="py-1.5 pr-4 font-medium">{step.name}</td>
                                      <td className="py-1.5 pr-4">
                                        <span className="flex items-center gap-1.5">
                                          <span className={`w-2 h-2 rounded-full ${statusDot(step.status)}`} />
                                          {step.status}
                                        </span>
                                      </td>
                                      <td className="py-1.5 pr-4 font-mono">{formatDurationMs(step.duration_ms)}</td>
                                      <td className="py-1.5 pr-4 font-mono">
                                        {stepCost?.total_cost_usd != null ? `$${stepCost.total_cost_usd.toFixed(2)}` : '--'}
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
