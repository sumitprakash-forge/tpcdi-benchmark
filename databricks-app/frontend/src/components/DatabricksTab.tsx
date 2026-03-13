import { useState, useEffect, useCallback, useRef } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput, Checkbox } from './FormField'
import Toast from './Toast'

interface ToastMessage {
  id: number
  message: string
  type: 'success' | 'error' | 'info'
}

interface JobStatus {
  run_id: number
  state: string
  result_state: string | null
  state_message: string
  run_page_url: string
  start_time: number | null
  end_time: number | null
  execution_duration: number | null
  setup_duration: number | null
}

interface RunInfo {
  run_id: number
  run_name: string
  state: string
  result_state: string | null
  state_message: string
  run_page_url: string
  start_time: number | null
  end_time: number | null
  execution_duration: number | null
  setup_duration: number | null
}

interface LatestRun {
  run_id: number
  state: string
  result_state: string | null
  start_time: number | null
  execution_duration: number | null
}

interface JobInfo {
  job_id: number
  name: string
  created_time: number | null
  latest_run: LatestRun | null
}

const STORAGE_KEY = 'tpcdi_databricks_job_id'

interface PrereqStep {
  step: string
  name: string
  status: 'exists' | 'created' | 'missing' | 'error'
  error?: string
  sa_email?: string
  url?: string
}

interface PrereqResult {
  status: 'ready' | 'incomplete'
  steps: PrereqStep[]
  summary: {
    catalog: string
    credential: string
    sa_email: string
    external_location: string
    external_location_url: string
    schema: string
    volume: string
  }
}

interface PrereqCheckResource {
  name: string
  exists: boolean
  sa_email?: string
  url?: string
}

interface PrereqCheckResult {
  status: 'ready' | 'incomplete'
  resources: Record<string, PrereqCheckResource>
}

export default function DatabricksTab() {
  // Prerequisites state
  const [catalogName] = useState('gcp_tpcdi_classic')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [prereqCheck, setPrereqCheck] = useState<PrereqCheckResult | null>(null)
  const [prereqResult, setPrereqResult] = useState<PrereqResult | null>(null)
  const [loadingPrereqCheck, setLoadingPrereqCheck] = useState(false)
  const [loadingPrereqCreate, setLoadingPrereqCreate] = useState(false)

  // Benchmark config state
  const [loadType, setLoadType] = useState('batch')
  const [scaleFactor, setScaleFactor] = useState('10')
  const [targetCatalog, setTargetCatalog] = useState('gcp_tpcdi_classic')
  const [targetSchema, setTargetSchema] = useState('dw')
  const [batchId, setBatchId] = useState('')
  const [cloud, setCloud] = useState('GCP')
  const [xmlFormat, setXmlFormat] = useState('com.databricks.spark.xml')
  const [sparkVersion, setSparkVersion] = useState('17.3.x-scala2.13')
  const [workerNodeType, setWorkerNodeType] = useState('n2-highmem-16')
  const [driverNodeType, setDriverNodeType] = useState('n2-highmem-16')
  const [numWorkers, setNumWorkers] = useState(3)
  const [jarPath, setJarPath] = useState('')
  const [photon, setPhoton] = useState(true)
  const [logDetailedStats, setLogDetailedStats] = useState(true)

  // GCP node type options
  const gcpNodeTypes = [
    { value: 'n2d-standard-4', label: 'n2d-standard-4 (4 vCPU, 16 GB)' },
    { value: 'n2d-standard-8', label: 'n2d-standard-8 (8 vCPU, 32 GB)' },
    { value: 'n2d-standard-16', label: 'n2d-standard-16 (16 vCPU, 64 GB)' },
    { value: 'n2d-standard-32', label: 'n2d-standard-32 (32 vCPU, 128 GB)' },
    { value: 'n2d-standard-48', label: 'n2d-standard-48 (48 vCPU, 192 GB)' },
    { value: 'n2d-standard-64', label: 'n2d-standard-64 (64 vCPU, 256 GB)' },
    { value: 'n2d-standard-80', label: 'n2d-standard-80 (80 vCPU, 320 GB)' },
    { value: 'n2d-standard-96', label: 'n2d-standard-96 (96 vCPU, 384 GB)' },
    { value: 'n2d-highmem-4', label: 'n2d-highmem-4 (4 vCPU, 32 GB)' },
    { value: 'n2d-highmem-8', label: 'n2d-highmem-8 (8 vCPU, 64 GB)' },
    { value: 'n2d-highmem-16', label: 'n2d-highmem-16 (16 vCPU, 128 GB)' },
    { value: 'n2d-highmem-32', label: 'n2d-highmem-32 (32 vCPU, 256 GB)' },
    { value: 'n2d-highmem-48', label: 'n2d-highmem-48 (48 vCPU, 384 GB)' },
    { value: 'n2d-highmem-64', label: 'n2d-highmem-64 (64 vCPU, 512 GB)' },
    { value: 'n2d-highmem-80', label: 'n2d-highmem-80 (80 vCPU, 640 GB)' },
    { value: 'n2d-highmem-96', label: 'n2d-highmem-96 (96 vCPU, 768 GB)' },
    { value: 'n2-highmem-16', label: 'n2-highmem-16 (16 vCPU, 128 GB)' },
    { value: 'c2-standard-4', label: 'c2-standard-4 (4 vCPU, 16 GB)' },
    { value: 'c2-standard-8', label: 'c2-standard-8 (8 vCPU, 32 GB)' },
    { value: 'c2-standard-16', label: 'c2-standard-16 (16 vCPU, 64 GB)' },
    { value: 'c2-standard-30', label: 'c2-standard-30 (30 vCPU, 120 GB)' },
  ]

  // Sizing suggestions based on load type and scale factor
  interface SizingConfig {
    driver: string
    worker: string
    workers: number
    label: string
  }

  const sizingConfig: SizingConfig | null = (() => {
    const sf = parseInt(scaleFactor)
    if (loadType === 'batch') {
      if (sf <= 10) return { driver: 'n2d-standard-4', worker: 'n2d-standard-4', workers: 2, label: 'Small batch' }
      if (sf <= 100) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 2, label: 'Medium batch' }
      if (sf <= 1000) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 5, label: 'Large batch' }
      return { driver: 'n2d-standard-8', worker: 'n2d-standard-16', workers: 6, label: 'XL batch' }
    } else {
      if (sf <= 10) return { driver: 'n2d-standard-4', worker: 'n2d-standard-4', workers: 2, label: 'Small incremental' }
      if (sf <= 100) return { driver: 'n2d-standard-8', worker: 'n2d-standard-4', workers: 2, label: 'Medium incremental' }
      if (sf <= 1000) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, label: 'Large incremental' }
      return { driver: 'n2d-standard-8', worker: 'n2d-standard-16', workers: 5, label: 'XL incremental' }
    }
  })()

  // Apply sizing suggestion when load type or scale factor changes
  useEffect(() => {
    if (sizingConfig) {
      setDriverNodeType(sizingConfig.driver)
      setWorkerNodeType(sizingConfig.worker)
      setNumWorkers(sizingConfig.workers)
    }
  }, [loadType, scaleFactor])

  // Run state - restore jobId from localStorage
  const [jobId, setJobId] = useState<number | null>(() => {
    const saved = localStorage.getItem(STORAGE_KEY)
    return saved ? parseInt(saved, 10) : null
  })
  const [runId, setRunId] = useState<number | null>(null)
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null)
  const [jobRuns, setJobRuns] = useState<RunInfo[]>([])
  const [loadingRuns, setLoadingRuns] = useState(false)
  const [availableJobs, setAvailableJobs] = useState<JobInfo[]>([])
  const [loadingJobs, setLoadingJobs] = useState(false)
  const [manualJobId, setManualJobId] = useState('')

  // Persist jobId to localStorage
  useEffect(() => {
    if (jobId) {
      localStorage.setItem(STORAGE_KEY, String(jobId))
    } else {
      localStorage.removeItem(STORAGE_KEY)
    }
  }, [jobId])

  // Loading states
  const [loadingJob, setLoadingJob] = useState(false)
  const [loadingRun, setLoadingRun] = useState(false)

  // Toast state
  const [toasts, setToasts] = useState<ToastMessage[]>([])
  const toastId = useRef(0)

  const addToast = useCallback((message: string, type: 'success' | 'error' | 'info') => {
    const id = ++toastId.current
    setToasts((prev) => [...prev, { id, message, type }])
  }, [])

  const removeToast = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id))
  }, [])

  async function apiCall(url: string, options?: RequestInit) {
    const res = await fetch(url, {
      ...options,
      headers: { 'Content-Type': 'application/json', ...options?.headers },
    })
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }))
      throw new Error(err.detail || 'Request failed')
    }
    return res.json()
  }

  // Sync targetCatalog with catalogName
  useEffect(() => {
    setTargetCatalog(catalogName)
  }, [catalogName])

  // Compute default jar path
  const effectiveJarPath =
    jarPath || `/Volumes/${targetCatalog}/${targetSchema}/libs/spark-xml_2.13-0.18.0.jar`

  // ── Prerequisites check on mount ──────────────────────────────────────

  const checkPrerequisites = useCallback(async (catName?: string) => {
    const cat = catName || catalogName
    setLoadingPrereqCheck(true)
    try {
      const data = await apiCall(`/api/databricks/check-prerequisites?catalog_name=${encodeURIComponent(cat)}&schema_name=${encodeURIComponent(targetSchema)}&raw_data_path=${encodeURIComponent(rawDataPath)}`)
      setPrereqCheck(data)
    } catch {
      // ignore
    } finally {
      setLoadingPrereqCheck(false)
    }
  }, [catalogName, targetSchema, rawDataPath])

  useEffect(() => {
    checkPrerequisites()
  }, [])

  async function handleCreatePrerequisites() {
    setLoadingPrereqCreate(true)
    setPrereqResult(null)
    try {
      const data = await apiCall('/api/databricks/verify-prerequisites', {
        method: 'POST',
        body: JSON.stringify({
          catalog_name: catalogName,
          schema_name: targetSchema,
          raw_data_path: rawDataPath,
        }),
      })
      setPrereqResult(data)
      if (data.status === 'ready') {
        addToast('All prerequisites are ready!', 'success')
      } else {
        const errors = (data.steps || []).filter((s: PrereqStep) => s.status === 'error' || s.status === 'missing')
        addToast(`${errors.length} prerequisite(s) need attention`, 'error')
      }
      // Refresh check
      await checkPrerequisites()
    } catch (e: any) {
      addToast(`Prerequisites failed: ${e.message}`, 'error')
    } finally {
      setLoadingPrereqCreate(false)
    }
  }

  // ── Load available jobs and runs on mount & when jobId changes ──────────

  const loadJobRuns = useCallback(async (jid: number) => {
    setLoadingRuns(true)
    try {
      const data = await apiCall(`/api/databricks/job-runs/${jid}?limit=15`)
      setJobRuns(data.runs || [])
      // If there's an active run, start polling it
      const activeRun = (data.runs || []).find(
        (r: RunInfo) => r.state === 'RUNNING' || r.state === 'PENDING' || r.state === 'QUEUED'
      )
      if (activeRun) {
        setRunId(activeRun.run_id)
      }
    } catch (e: any) {
      console.error('Failed to load runs:', e)
    } finally {
      setLoadingRuns(false)
    }
  }, [])

  const loadAvailableJobs = useCallback(async () => {
    setLoadingJobs(true)
    try {
      const data = await apiCall('/api/databricks/list-jobs')
      setAvailableJobs(data.jobs || [])
    } catch {
      // ignore
    } finally {
      setLoadingJobs(false)
    }
  }, [])

  useEffect(() => {
    loadAvailableJobs()
  }, [])

  useEffect(() => {
    if (jobId) loadJobRuns(jobId)
  }, [jobId, loadJobRuns])

  // ── Job Actions ──────────────────────────────────────────────────────────

  async function handleCreateJob() {
    setLoadingJob(true)
    try {
      const result = await apiCall('/api/databricks/create-job', {
        method: 'POST',
        body: JSON.stringify({
          job_name: `TPC-DI Benchmark SF${scaleFactor} ${loadType}`,
          load_type: loadType,
          scale_factor: parseInt(scaleFactor),
          raw_data_path: rawDataPath,
          catalog: targetCatalog,
          schema_name: targetSchema,
          batch_id: batchId || '',
          cloud,
          xml_format: xmlFormat,
          spark_version: sparkVersion,
          worker_node_type: workerNodeType,
          driver_node_type: driverNodeType,
          num_workers: numWorkers,
          jar_path: effectiveJarPath,
          photon,
          log_detailed_stats: logDetailedStats,
          job_id: jobId,
        }),
      })
      setJobId(result.job_id)
      addToast(`Job ${result.action}: ID ${result.job_id}`, 'success')
    } catch (e: any) {
      addToast(`Failed to create/update job: ${e.message}`, 'error')
    } finally {
      setLoadingJob(false)
    }
  }

  async function handleRunBenchmark() {
    if (!jobId) {
      addToast('Create a job first', 'error')
      return
    }
    setLoadingRun(true)
    try {
      const result = await apiCall('/api/databricks/run-job', {
        method: 'POST',
        body: JSON.stringify({ job_id: jobId }),
      })
      setRunId(result.run_id)
      addToast(`Benchmark started: Run ID ${result.run_id}`, 'success')
      // Refresh run list
      setTimeout(() => loadJobRuns(jobId), 2000)
    } catch (e: any) {
      addToast(`Failed to start benchmark: ${e.message}`, 'error')
    } finally {
      setLoadingRun(false)
    }
  }

  function handleLoadJobId() {
    const parsed = parseInt(manualJobId, 10)
    if (parsed) {
      setJobId(parsed)
      setManualJobId('')
      addToast(`Loaded job ID: ${parsed}`, 'info')
    }
  }

  // ── Status Polling ─────────────────────────────────────────────────────

  const pollStatus = useCallback(async () => {
    if (!runId) return
    try {
      const status = await apiCall(`/api/databricks/job-status/${runId}`)
      setJobStatus(status)
      // Update the run in jobRuns list too
      setJobRuns((prev) =>
        prev.map((r) =>
          r.run_id === runId
            ? {
                ...r,
                state: status.state,
                result_state: status.result_state,
                state_message: status.state_message,
                execution_duration: status.execution_duration,
                setup_duration: status.setup_duration,
              }
            : r
        )
      )
    } catch {
      // Ignore polling errors
    }
  }, [runId])

  useEffect(() => {
    if (!runId) return
    pollStatus()
    const interval = setInterval(pollStatus, 10000)
    return () => clearInterval(interval)
  }, [runId, pollStatus])

  const isTerminal =
    jobStatus?.state === 'TERMINATED' ||
    jobStatus?.state === 'SKIPPED' ||
    jobStatus?.state === 'INTERNAL_ERROR'

  // When current run finishes, refresh the runs list
  useEffect(() => {
    if (isTerminal && jobId) {
      loadJobRuns(jobId)
    }
  }, [isTerminal, jobId, loadJobRuns])

  function formatDuration(ms: number | null | undefined) {
    if (!ms) return '--'
    const seconds = Math.floor(ms / 1000)
    const minutes = Math.floor(seconds / 60)
    const hours = Math.floor(minutes / 60)
    if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`
    return `${seconds}s`
  }

  function formatTimestamp(ts: number | null | undefined) {
    if (!ts) return '--'
    return new Date(ts).toLocaleString()
  }

  function stateColor(state: string, resultState: string | null | undefined) {
    if (state === 'TERMINATED' && resultState === 'SUCCESS') return 'bg-green-100 text-green-700'
    if (state === 'TERMINATED') return 'bg-red-100 text-red-700'
    if (state === 'RUNNING') return 'bg-blue-100 text-blue-700'
    if (state === 'PENDING' || state === 'QUEUED') return 'bg-yellow-100 text-yellow-700'
    if (state === 'SKIPPED') return 'bg-slate-100 text-slate-600'
    if (state === 'INTERNAL_ERROR') return 'bg-red-100 text-red-700'
    return 'bg-slate-100 text-slate-600'
  }

  function stateLabel(state: string, resultState: string | null | undefined) {
    if (state === 'TERMINATED' && resultState === 'SUCCESS') return 'SUCCESS'
    if (state === 'TERMINATED') return resultState || 'FAILED'
    if (state === 'INTERNAL_ERROR') return 'ERROR'
    return state
  }

  return (
    <div>
      {toasts.map((t) => (
        <Toast key={t.id} message={t.message} type={t.type} onClose={() => removeToast(t.id)} />
      ))}

      {/* Prerequisites Section */}
      <Section title="1. Create & Verify Prerequisites" defaultOpen={true} colorScheme="emerald">
        <div className="pt-4">
          {/* Status check results */}
          {prereqCheck && (
            <div className={`mb-4 p-4 rounded-lg border ${prereqCheck.status === 'ready' ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'}`}>
              <div className="flex items-center justify-between mb-3">
                <h4 className={`text-sm font-semibold ${prereqCheck.status === 'ready' ? 'text-green-700' : 'text-amber-700'}`}>
                  {prereqCheck.status === 'ready' ? 'All prerequisites ready' : 'Some prerequisites missing'}
                </h4>
                <button
                  onClick={() => checkPrerequisites()}
                  disabled={loadingPrereqCheck}
                  className="text-xs text-slate-500 hover:text-slate-700 disabled:opacity-50"
                >
                  {loadingPrereqCheck ? 'Checking...' : 'Re-check'}
                </button>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
                {Object.entries(prereqCheck.resources).map(([key, res]) => (
                  <div key={key} className="flex items-start gap-2">
                    <span className={`mt-0.5 w-4 h-4 rounded-full flex items-center justify-center text-[10px] font-bold ${res.exists ? 'bg-green-500 text-white' : 'bg-slate-300 text-white'}`}>
                      {res.exists ? '\u2713' : '\u00d7'}
                    </span>
                    <div>
                      <p className="text-xs font-medium text-slate-700 capitalize">{key.replace('_', ' ')}</p>
                      <p className="text-[10px] text-slate-500 font-mono truncate max-w-[140px]" title={res.name}>{res.name}</p>
                      {res.sa_email && <p className="text-[10px] text-slate-400 truncate max-w-[140px]" title={res.sa_email}>{res.sa_email}</p>}
                      {res.url && <p className="text-[10px] text-slate-400 truncate max-w-[140px]" title={res.url}>{res.url}</p>}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Config inputs */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <FormField label="Raw Data Path (GCS)">
              <TextInput value={rawDataPath} onChange={setRawDataPath} placeholder="gs://bucket/tpcdi" />
            </FormField>
          </div>

          {/* Create & Verify button */}
          <div className="flex flex-wrap gap-3 mt-5 pt-4 border-t border-slate-100">
            <ActionButton
              label="Create & Verify Prerequisites"
              onClick={handleCreatePrerequisites}
              loading={loadingPrereqCreate}
            />
            <ActionButton
              label="Check Status"
              onClick={() => checkPrerequisites()}
              loading={loadingPrereqCheck}
              variant="secondary"
            />
          </div>

          {/* Step-by-step results from create */}
          {prereqResult && (
            <div className="mt-4 border border-slate-200 rounded-lg overflow-hidden">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-slate-50 text-left text-slate-500 uppercase tracking-wide">
                    <th className="px-3 py-2">Resource</th>
                    <th className="px-3 py-2">Name</th>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Details</th>
                  </tr>
                </thead>
                <tbody>
                  {prereqResult.steps.map((s, i) => (
                    <tr key={i} className="border-t border-slate-100">
                      <td className="px-3 py-2 font-medium text-slate-700 capitalize">{s.step.replace('_', ' ')}</td>
                      <td className="px-3 py-2 font-mono text-slate-600">{s.name}</td>
                      <td className="px-3 py-2">
                        <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${
                          s.status === 'exists' ? 'bg-green-100 text-green-700' :
                          s.status === 'created' ? 'bg-blue-100 text-blue-700' :
                          s.status === 'missing' ? 'bg-amber-100 text-amber-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {s.status.toUpperCase()}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-slate-500 max-w-xs truncate">
                        {s.error || s.sa_email || s.url || '--'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {/* Summary */}
              {prereqResult.summary && prereqResult.status === 'ready' && (
                <div className="bg-green-50 px-4 py-3 border-t border-slate-200">
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 text-xs">
                    <div>
                      <span className="text-slate-500">Catalog:</span>{' '}
                      <span className="font-mono font-medium text-slate-700">{prereqResult.summary.catalog}</span>
                    </div>
                    <div>
                      <span className="text-slate-500">SA Email:</span>{' '}
                      <span className="font-mono font-medium text-slate-700 truncate">{prereqResult.summary.sa_email || '--'}</span>
                    </div>
                    <div>
                      <span className="text-slate-500">External Location:</span>{' '}
                      <span className="font-mono font-medium text-slate-700">{prereqResult.summary.external_location}</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </Section>

      {/* Benchmark Configuration Section - visible only when prerequisites are ready */}
      {prereqCheck?.status === 'ready' && (
      <Section title="2. Benchmark Configuration" colorScheme="amber">
        {/* Sizing Suggestion Banner */}
        {sizingConfig && (
          <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg">
            <p className="text-sm text-amber-800">
              <span className="font-semibold">Auto-sized for {loadType} SF={scaleFactor} ({sizingConfig.label}):</span>{' '}
              Driver <span className="font-mono">{sizingConfig.driver}</span>, Worker <span className="font-mono">{sizingConfig.worker}</span>, {sizingConfig.workers} workers
            </p>
          </div>
        )}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 pt-4">
          <FormField label="Load Type">
            <SelectInput
              value={loadType}
              onChange={setLoadType}
              options={[
                { value: 'batch', label: 'Batch (Historical Load)' },
                { value: 'incremental', label: 'Incremental' },
              ]}
            />
          </FormField>
          <FormField label="Scale Factor">
            <SelectInput
              value={scaleFactor}
              onChange={setScaleFactor}
              options={[
                { value: '10', label: '10' },
                { value: '100', label: '100' },
                { value: '1000', label: '1000' },
                { value: '10000', label: '10000' },
              ]}
            />
          </FormField>
          {loadType === 'incremental' && (
            <FormField label="Batch ID" hint="Incremental batch: 2 or 3">
              <SelectInput
                value={batchId}
                onChange={setBatchId}
                options={[
                  { value: '2', label: 'Batch 2' },
                  { value: '3', label: 'Batch 3' },
                ]}
              />
            </FormField>
          )}
          <FormField label="Target Catalog" hint="Set in Prerequisites">
            <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700">{targetCatalog}</div>
          </FormField>
          <FormField label="Raw Data Path" hint="Set in Prerequisites">
            <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700 truncate" title={rawDataPath}>{rawDataPath}</div>
          </FormField>
          <FormField label="Target Schema">
            <TextInput value={targetSchema} onChange={setTargetSchema} placeholder="dw" />
          </FormField>
          <FormField label="Cloud">
            <SelectInput
              value={cloud}
              onChange={setCloud}
              options={[
                { value: 'GCP', label: 'GCP' },
                { value: 'AWS', label: 'AWS' },
                { value: 'Azure', label: 'Azure' },
              ]}
            />
          </FormField>
          <FormField label="XML Format">
            <SelectInput
              value={xmlFormat}
              onChange={setXmlFormat}
              options={[
                { value: 'com.databricks.spark.xml', label: 'com.databricks.spark.xml' },
                {
                  value: 'org.apache.spark.sql.execution.datasources.xml',
                  label: 'org.apache.spark.sql.execution.datasources.xml',
                },
              ]}
            />
          </FormField>
          <FormField label="Spark Version">
            <SelectInput
              value={sparkVersion}
              onChange={setSparkVersion}
              options={[
                { value: '17.3.x-scala2.13', label: '17.3.x-scala2.13 (LTS)' },
                { value: '16.4.x-scala2.12', label: '16.4.x-scala2.12 (LTS)' },
                { value: '15.4.x-scala2.12', label: '15.4.x-scala2.12 (LTS)' },
              ]}
            />
          </FormField>
          <FormField label="Worker Node Type">
            <SelectInput value={workerNodeType} onChange={setWorkerNodeType} options={gcpNodeTypes} />
          </FormField>
          <FormField label="Driver Node Type">
            <SelectInput value={driverNodeType} onChange={setDriverNodeType} options={gcpNodeTypes} />
          </FormField>
          <FormField label="Number of Workers">
            <NumberInput value={numWorkers} onChange={setNumWorkers} min={1} max={100} />
          </FormField>
          <div className="lg:col-span-3">
            <FormField label="JAR Library Path" hint={`Default: ${effectiveJarPath}`}>
              <TextInput value={jarPath} onChange={setJarPath} placeholder={effectiveJarPath} />
            </FormField>
          </div>
          <div className="flex gap-6">
            <Checkbox checked={photon} onChange={setPhoton} label="Photon Enabled" />
            <Checkbox checked={logDetailedStats} onChange={setLogDetailedStats} label="Log Detailed Stats" />
          </div>
        </div>

      </Section>
      )}

      {/* Run Section - visible only when prerequisites are ready */}
      {prereqCheck?.status === 'ready' && (
      <Section title="3. Run Benchmark" defaultOpen={true} colorScheme="blue">
        <div className="pt-4">
          {/* Job selector / loader */}
          <div className="flex flex-wrap gap-3 items-center mb-4">
            <ActionButton label={jobId ? 'Update Job' : 'Create Job'} onClick={handleCreateJob} loading={loadingJob} />
            <ActionButton
              label="Run Benchmark"
              onClick={handleRunBenchmark}
              loading={loadingRun}
              disabled={!jobId}
              variant="secondary"
            />
            {jobId && (
              <span className="text-sm text-slate-500">
                Job ID: <span className="font-mono font-medium text-slate-700">{jobId}</span>
                <button
                  onClick={() => { setJobId(null); setRunId(null); setJobStatus(null); setJobRuns([]) }}
                  className="ml-2 text-xs text-red-400 hover:text-red-600"
                  title="Clear job"
                >
                  clear
                </button>
              </span>
            )}
          </div>

          {/* Benchmark Jobs List - always visible */}
          <div className="mb-4">
            <div className="flex items-center justify-between mb-2">
              <h4 className="text-sm font-semibold text-slate-700">Benchmark Jobs</h4>
              <button
                onClick={loadAvailableJobs}
                disabled={loadingJobs}
                className="text-xs text-orange-600 hover:text-orange-700 disabled:opacity-50"
              >
                {loadingJobs ? 'Loading...' : 'Refresh'}
              </button>
            </div>
            {/* Manual job ID input */}
            <div className="flex gap-2 items-center mb-3">
              <input
                type="text"
                value={manualJobId}
                onChange={(e) => setManualJobId(e.target.value)}
                placeholder="Enter Job ID manually"
                className="border border-slate-300 rounded-md px-3 py-1.5 text-sm w-48 focus:outline-none focus:ring-2 focus:ring-orange-500 focus:border-transparent"
                onKeyDown={(e) => e.key === 'Enter' && handleLoadJobId()}
              />
              <ActionButton label="Load" onClick={handleLoadJobId} variant="secondary" />
            </div>
            {availableJobs.length > 0 ? (
              <div className="border border-slate-200 rounded-lg overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-slate-50 text-left text-slate-500 uppercase tracking-wide">
                      <th className="px-3 py-2">Job ID</th>
                      <th className="px-3 py-2">Name</th>
                      <th className="px-3 py-2">Latest Run</th>
                      <th className="px-3 py-2">Last Run Time</th>
                      <th className="px-3 py-2">Duration</th>
                      <th className="px-3 py-2"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {availableJobs.map((j) => (
                      <tr
                        key={j.job_id}
                        className={`border-t border-slate-100 hover:bg-slate-50 cursor-pointer ${j.job_id === jobId ? 'bg-orange-50 border-l-2 border-l-orange-400' : ''}`}
                        onClick={() => setJobId(j.job_id)}
                      >
                        <td className="px-3 py-2 font-mono text-orange-600">{j.job_id}</td>
                        <td className="px-3 py-2 text-slate-700 font-medium">{j.name}</td>
                        <td className="px-3 py-2">
                          {j.latest_run ? (
                            <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${stateColor(j.latest_run.state, j.latest_run.result_state)}`}>
                              {stateLabel(j.latest_run.state, j.latest_run.result_state)}
                            </span>
                          ) : (
                            <span className="text-slate-400">No runs</span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-slate-500">
                          {j.latest_run ? formatTimestamp(j.latest_run.start_time) : '--'}
                        </td>
                        <td className="px-3 py-2 font-mono">
                          {j.latest_run ? formatDuration(j.latest_run.execution_duration) : '--'}
                        </td>
                        <td className="px-3 py-2">
                          <button
                            onClick={(e) => { e.stopPropagation(); setJobId(j.job_id) }}
                            className={`text-xs px-2 py-1 rounded ${j.job_id === jobId ? 'bg-orange-100 text-orange-700' : 'bg-slate-100 text-slate-600 hover:bg-orange-50 hover:text-orange-600'}`}
                          >
                            {j.job_id === jobId ? 'Selected' : 'Select'}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : !loadingJobs ? (
              <p className="text-sm text-slate-400">No benchmark jobs found. Create one above.</p>
            ) : null}
          </div>

          {/* Current Run Status */}
          {jobStatus && runId && (
            <div className="mb-4 p-4 bg-slate-50 rounded-lg border border-slate-200">
              <h4 className="text-sm font-semibold text-slate-700 mb-3">Current Run</h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-xs text-slate-500 uppercase">State</p>
                  <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${stateColor(jobStatus.state, jobStatus.result_state)}`}>
                    {stateLabel(jobStatus.state, jobStatus.result_state)}
                  </span>
                </div>
                <div>
                  <p className="text-xs text-slate-500 uppercase">Run ID</p>
                  <p className="text-sm font-mono">{jobStatus.run_id}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500 uppercase">Setup Duration</p>
                  <p className="text-sm font-mono">{formatDuration(jobStatus.setup_duration)}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500 uppercase">Execution Duration</p>
                  <p className="text-sm font-mono">{formatDuration(jobStatus.execution_duration)}</p>
                </div>
              </div>
              {jobStatus.state_message && (
                <div className="mt-3">
                  <p className="text-xs text-slate-500 uppercase">Message</p>
                  <p className="text-sm text-slate-600 mt-1 break-words">{jobStatus.state_message}</p>
                </div>
              )}
              {jobStatus.run_page_url && (
                <a
                  href={jobStatus.run_page_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-block mt-2 text-sm text-orange-600 hover:text-orange-700 underline"
                >
                  View Run in Databricks
                </a>
              )}
              {!isTerminal && (
                <p className="mt-2 text-xs text-slate-400">Auto-refreshing every 10 seconds...</p>
              )}
            </div>
          )}

          {/* Run History */}
          {jobId && (
            <div>
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-sm font-semibold text-slate-700">Run History</h4>
                <button
                  onClick={() => loadJobRuns(jobId)}
                  disabled={loadingRuns}
                  className="text-xs text-orange-600 hover:text-orange-700 disabled:opacity-50"
                >
                  {loadingRuns ? 'Refreshing...' : 'Refresh'}
                </button>
              </div>
              {jobRuns.length === 0 && !loadingRuns && (
                <p className="text-sm text-slate-400">No runs yet. Click "Run Benchmark" to start one.</p>
              )}
              {jobRuns.length > 0 && (
                <div className="border border-slate-200 rounded-lg overflow-hidden">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-slate-50 text-left text-slate-500 uppercase tracking-wide">
                        <th className="px-3 py-2">Status</th>
                        <th className="px-3 py-2">Run ID</th>
                        <th className="px-3 py-2">Started</th>
                        <th className="px-3 py-2">Setup</th>
                        <th className="px-3 py-2">Execution</th>
                        <th className="px-3 py-2">Message</th>
                        <th className="px-3 py-2"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {jobRuns.map((r) => (
                        <tr
                          key={r.run_id}
                          className={`border-t border-slate-100 hover:bg-slate-50 ${r.run_id === runId ? 'bg-orange-50' : ''}`}
                        >
                          <td className="px-3 py-2">
                            <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${stateColor(r.state, r.result_state)}`}>
                              {stateLabel(r.state, r.result_state)}
                            </span>
                          </td>
                          <td className="px-3 py-2 font-mono">{r.run_id}</td>
                          <td className="px-3 py-2 text-slate-500">{formatTimestamp(r.start_time)}</td>
                          <td className="px-3 py-2 font-mono">{formatDuration(r.setup_duration)}</td>
                          <td className="px-3 py-2 font-mono">{formatDuration(r.execution_duration)}</td>
                          <td className="px-3 py-2 text-slate-500 max-w-xs truncate" title={r.state_message}>
                            {r.state_message ? r.state_message.slice(0, 80) + (r.state_message.length > 80 ? '...' : '') : '--'}
                          </td>
                          <td className="px-3 py-2">
                            {r.run_page_url && (
                              <a
                                href={r.run_page_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-orange-600 hover:text-orange-700 underline"
                              >
                                View
                              </a>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      </Section>
      )}
    </div>
  )
}
