import { useState, useEffect, useCallback, useRef } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput } from './FormField'
import Toast from './Toast'

interface ToastMessage {
  id: number
  message: string
  type: 'success' | 'error' | 'info'
}

interface PrereqDetail {
  label: string
  name: string
  status: string
  error?: string
}

interface E2EStep {
  name: string
  status: 'pending' | 'running' | 'success' | 'error'
  output: string
  duration_ms: number | null
  link?: string
  details?: PrereqDetail[]
}

interface DataprocRunCost {
  vm_cost_usd: number
  dataproc_premium_usd: number
  ssd_cost_usd: number
  total_cost_usd: number
  duration_hours: number
  duration_ms: number
  cluster: {
    master_type: string
    worker_type: string
    num_workers: number
    ssd_count: number
  }
}

interface DataprocRunStatus {
  run_id: string
  phase: string
  state: string
  cluster_name: string
  job_id: string | null
  error: string | null
  start_time_ms: number | null
  cluster_created_ms: number | null
  job_submitted_ms: number | null
  job_done_ms: number | null
  cluster_deleted_ms: number | null
  duration_ms: number | null
  cost: DataprocRunCost | null
  config: {
    master_type: string
    worker_type: string
    num_workers: number
    ssd_count: number
    load_type: string
    batch_id: string
    scale_factor: number
  }
}

interface E2ERunSpec {
  loadType: string
  batchId: string
  stepIndex: number
}

interface E2EPersistedState {
  steps: E2EStep[]
  sf: string
  runId: string | null
  activeStep: number | null
  running: boolean
  costs: (DataprocRunCost | null)[]
  remainingSpecs: E2ERunSpec[]
}

const E2E_STATE_KEY = 'tpcdi_dataproc_e2e_state'

const E2E_RUN_SPECS: E2ERunSpec[] = [
  { loadType: 'batch', batchId: '', stepIndex: 1 },
  { loadType: 'incremental', batchId: '2', stepIndex: 2 },
  { loadType: 'incremental', batchId: '3', stepIndex: 3 },
]

function getDataprocSizing(sf: number, loadType: string) {
  // Match sizing from run_tpcdi_cycle_dataproc_classic.sh
  if (loadType === 'batch') {
    if (sf <= 100) return { master: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    if (sf <= 1000) return { master: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 5, ssds: 2 }
    return { master: 'n2d-standard-8', worker: 'n2d-standard-16', workers: 6, ssds: 4 }
  } else {
    if (sf <= 100) return { master: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    if (sf <= 1000) return { master: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    return { master: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 5, ssds: 2 }
  }
}

function buildE2ESteps(sf: string): E2EStep[] {
  return [
    { name: 'Check gcloud & Network', status: 'pending', output: '', duration_ms: null },
    { name: `Batch Load (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
    { name: `Incremental Load - Batch 2 (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
    { name: `Incremental Load - Batch 3 (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
  ]
}

export default function DataprocTab() {
  const [mode, setMode] = useState<'e2e' | 'manual'>('e2e')

  // Common config
  const [project, setProject] = useState('gcp-sandbox-field-eng')
  const [region, setRegion] = useState('us-central1')
  const [zone, setZone] = useState('us-central1-a')
  const [subnet, setSubnet] = useState('tpcdi-benchmark-subnet')
  const [metastoreMode, setMetastoreMode] = useState<'spark_default' | 'create' | 'existing'>('spark_default')
  const [metastore, setMetastore] = useState('')
  const [metastoreName, setMetastoreName] = useState('tpcdi-benchmark-metastore')
  const [gcsBucket, setGcsBucket] = useState('sumit_prakash_gcs')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [targetDatabase, setTargetDatabase] = useState('tpcdi_warehouse')
  const [metricsOutput, setMetricsOutput] = useState('gs://sumit_prakash_gcs/tpcdi/metrics')

  // Manual mode config
  const [clusterName, setClusterName] = useState('tpcdi-dataproc-cluster')
  const [masterType, setMasterType] = useState('n2d-standard-8')
  const [workerType, setWorkerType] = useState('n2d-standard-8')
  const [numWorkers, setNumWorkers] = useState(3)
  const [ssdCount, setSsdCount] = useState(2)
  const [loadType, setLoadType] = useState('batch')
  const [scaleFactor, setScaleFactor] = useState('1000')
  const [batchId, setBatchId] = useState('')

  // Manual run state
  const [manualRunId, setManualRunId] = useState<string | null>(null)
  const [manualRunStatus, setManualRunStatus] = useState<DataprocRunStatus | null>(null)

  // Google auth state
  const [gcpAuth, setGcpAuth] = useState<{ status: string; method?: string; account?: string; message?: string } | null>(null)
  const [loadingAuth, setLoadingAuth] = useState(false)
  const [showTokenInput, setShowTokenInput] = useState(false)
  const [tokenInput, setTokenInput] = useState('')
  const [tokenType, setTokenType] = useState<'refresh_token' | 'access_token' | 'adc_json'>('refresh_token')

  // E2E state - restore from localStorage
  const savedE2E = useRef<E2EPersistedState | null>((() => {
    try {
      const raw = localStorage.getItem(E2E_STATE_KEY)
      return raw ? JSON.parse(raw) : null
    } catch { return null }
  })())
  const [e2eSteps, setE2eSteps] = useState<E2EStep[]>(savedE2E.current?.steps || buildE2ESteps('1000'))
  const [e2eRunning, setE2eRunning] = useState(savedE2E.current?.running || false)
  const [e2eSF, setE2eSF] = useState(savedE2E.current?.sf || '1000')
  const [e2eRunId, setE2eRunId] = useState<string | null>(savedE2E.current?.runId || null)
  const [e2eActiveStep, setE2eActiveStep] = useState<number | null>(savedE2E.current?.activeStep || null)
  const [runCosts, setRunCosts] = useState<(DataprocRunCost | null)[]>(savedE2E.current?.costs || [null, null, null])
  const [e2eRemainingSpecs, setE2eRemainingSpecs] = useState<E2ERunSpec[]>(savedE2E.current?.remainingSpecs || [])

  // Persist E2E state
  useEffect(() => {
    const state: E2EPersistedState = {
      steps: e2eSteps, sf: e2eSF, runId: e2eRunId, activeStep: e2eActiveStep,
      running: e2eRunning, costs: runCosts, remainingSpecs: e2eRemainingSpecs,
    }
    localStorage.setItem(E2E_STATE_KEY, JSON.stringify(state))
  }, [e2eSteps, e2eSF, e2eRunId, e2eActiveStep, e2eRunning, runCosts, e2eRemainingSpecs])

  // Loading / Toast
  const [loadingManualRun, setLoadingManualRun] = useState(false)
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

  function formatDuration(ms: number | null | undefined) {
    if (!ms) return '--'
    const seconds = Math.floor(ms / 1000)
    const minutes = Math.floor(seconds / 60)
    const hours = Math.floor(minutes / 60)
    if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`
    return `${seconds}s`
  }

  function formatCost(usd: number) {
    return `$${usd.toFixed(4)}`
  }

  const gcpNodeTypes = [
    { value: 'n2d-standard-4', label: 'n2d-standard-4 (4 vCPU)' },
    { value: 'n2d-standard-8', label: 'n2d-standard-8 (8 vCPU)' },
    { value: 'n2d-standard-16', label: 'n2d-standard-16 (16 vCPU)' },
    { value: 'n2d-standard-32', label: 'n2d-standard-32 (32 vCPU)' },
    { value: 'n2d-highmem-4', label: 'n2d-highmem-4 (4 vCPU)' },
    { value: 'n2d-highmem-8', label: 'n2d-highmem-8 (8 vCPU)' },
    { value: 'n2d-highmem-16', label: 'n2d-highmem-16 (16 vCPU)' },
  ]

  // ── Google Cloud auth ────────────────────────────────────────────────

  const checkGcpAuth = useCallback(async () => {
    setLoadingAuth(true)
    try {
      const data = await apiCall('/api/dataproc/auth/status')
      setGcpAuth(data)
    } catch (e: any) {
      setGcpAuth({ status: 'error', message: e.message })
    } finally {
      setLoadingAuth(false)
    }
  }, [])

  useEffect(() => { checkGcpAuth() }, [])

  async function handleTokenSubmit() {
    if (!tokenInput.trim()) return
    setLoadingAuth(true)
    try {
      const data = await apiCall('/api/dataproc/auth/login', {
        method: 'POST',
        body: JSON.stringify({ token: tokenInput.trim(), token_type: tokenType }),
      })
      setGcpAuth(data)
      if (data.status === 'authenticated') {
        setShowTokenInput(false)
        setTokenInput('')
        addToast(`Authenticated as ${data.account}`, 'success')
      } else {
        addToast(data.message || 'Authentication failed', 'error')
      }
    } catch (e: any) {
      addToast(`Auth failed: ${e.message}`, 'error')
    } finally {
      setLoadingAuth(false)
    }
  }

  async function handleLogout() {
    await apiCall('/api/dataproc/auth/logout', { method: 'POST' })
    setGcpAuth({ status: 'unauthenticated', message: 'Logged out' })
    addToast('Google Cloud credentials cleared', 'info')
  }

  // ── E2E helpers ──────────────────────────────────────────────────────

  function updateE2EStep(index: number, updates: Partial<E2EStep>) {
    setE2eSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s))
  }

  const e2eCancelledRef = useRef(false)

  function buildRunLabel(spec: E2ERunSpec, sf: number): string {
    const sizing = getDataprocSizing(sf, spec.loadType)
    return spec.loadType === 'batch'
      ? `SF ${sf} | Batch | ${sizing.worker} x${sizing.workers}, ${sizing.ssds} SSDs`
      : `SF ${sf} | Incremental Batch ${spec.batchId} | ${sizing.worker} x${sizing.workers}, ${sizing.ssds} SSDs`
  }

  async function startE2ERunSpec(spec: E2ERunSpec) {
    const sf = parseInt(e2eSF)
    const { loadType, batchId, stepIndex } = spec
    const sizing = getDataprocSizing(sf, loadType)
    const runLabel = buildRunLabel(spec, sf)
    const clName = `tpcdi-dp-sf${sf}-${loadType}${batchId ? '-b' + batchId : ''}`

    // Set run ID to 'pending' immediately to prevent auto-continue from firing
    // before the API responds (race condition fix)
    setE2eRunId('pending')
    setE2eActiveStep(stepIndex)
    updateE2EStep(stepIndex, { status: 'running', output: `${runLabel} | Starting full cycle (create cluster -> run job -> delete cluster)...` })

    try {
      const result = await apiCall('/api/dataproc/run-full-cycle', {
        method: 'POST',
        body: JSON.stringify({
          cluster_name: clName,
          project, region, zone, subnet,
          metastore: metastoreMode === 'existing' ? metastore : (metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : ''),
          image_version: '2.3-debian12',
          master_type: sizing.master,
          worker_type: sizing.worker,
          num_workers: sizing.workers,
          ssd_count: sizing.ssds,
          load_type: loadType,
          batch_id: batchId,
          scale_factor: sf,
          gcs_bucket: gcsBucket,
          raw_data_path: rawDataPath,
          target_database: targetDatabase,
          metrics_output: metricsOutput,
        }),
      })
      setE2eRunId(result.run_id)
      updateE2EStep(stepIndex, { output: `${runLabel} | Run ${result.run_id} started. Polling...` })
    } catch (e: any) {
      setE2eRunId(null)
      updateE2EStep(stepIndex, { status: 'error', output: `${runLabel} | ${e.message}` })
      setE2eRunning(false)
      setE2eRemainingSpecs([])
    }
  }

  // Polling useEffect for E2E run
  useEffect(() => {
    // Skip polling for the 'pending' sentinel — wait for a real run ID
    if (!e2eRunId || e2eRunId === 'pending' || e2eActiveStep === null) return
    const stepIdx = e2eActiveStep
    const sf = parseInt(e2eSF)
    const spec = E2E_RUN_SPECS.find(s => s.stepIndex === stepIdx)
    const runLabel = spec ? buildRunLabel(spec, sf) : `Step ${stepIdx}`

    const phaseIcon = (phase: string) => {
      if (phase.includes('creating')) return 'Cluster provisioning'
      if (phase.includes('running_job')) return 'Job executing'
      if (phase.includes('deleting')) return 'Cluster teardown'
      if (phase === 'completed') return 'Complete'
      if (phase === 'failed') return 'Failed'
      return phase.replace(/_/g, ' ')
    }

    const poll = async () => {
      try {
        const status: DataprocRunStatus = await apiCall(`/api/dataproc/run-status/${e2eRunId}`)
        const elapsed = status.start_time_ms
          ? Date.now() - status.start_time_ms
          : null

        // Build detailed status line
        const parts: string[] = [runLabel]
        parts.push(`Phase: ${phaseIcon(status.phase)}`)
        if (status.cluster_created_ms && status.start_time_ms) {
          parts.push(`Cluster: ${formatDuration(status.cluster_created_ms - status.start_time_ms)}`)
        }
        if (status.job_submitted_ms && status.phase === 'running_job') {
          const jobElapsed = Date.now() - status.job_submitted_ms
          parts.push(`Job: ${formatDuration(jobElapsed)}`)
        }
        if (status.job_done_ms && status.job_submitted_ms) {
          parts.push(`Job: ${formatDuration(status.job_done_ms - status.job_submitted_ms)}`)
        }
        if (elapsed) {
          parts.push(`Total: ${formatDuration(elapsed)}`)
        }

        // Build GCP Console link for the Dataproc job
        const gcpLink = status.job_id && status.config
          ? `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
          : status.cluster_name
            ? `https://console.cloud.google.com/dataproc/clusters/${status.cluster_name}?project=${project}&region=${region}`
            : undefined

        updateE2EStep(stepIdx, {
          output: parts.join(' | '),
          duration_ms: elapsed,
          link: gcpLink,
        })

        if (status.state === 'DONE') {
          // Build completion summary with timing breakdown
          const timingParts: string[] = [runLabel, 'Done']
          if (status.cluster_created_ms && status.start_time_ms) {
            timingParts.push(`Cluster: ${formatDuration(status.cluster_created_ms - status.start_time_ms)}`)
          }
          if (status.job_done_ms && status.job_submitted_ms) {
            timingParts.push(`Job: ${formatDuration(status.job_done_ms - status.job_submitted_ms)}`)
          }
          if (status.cluster_deleted_ms && status.job_done_ms) {
            timingParts.push(`Teardown: ${formatDuration(status.cluster_deleted_ms - status.job_done_ms)}`)
          }
          timingParts.push(`Total: ${formatDuration(status.duration_ms)}`)
          if (status.cost) {
            timingParts.push(`Cost: $${status.cost.total_cost_usd.toFixed(2)}`)
          }

          const doneLink = status.job_id
            ? `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
            : undefined

          updateE2EStep(stepIdx, {
            status: 'success',
            output: timingParts.join(' | '),
            duration_ms: status.duration_ms,
            link: doneLink,
          })
          if (status.cost) {
            setRunCosts(prev => {
              const next = [...prev]
              next[stepIdx - 1] = status.cost
              return next
            })
          }
          setE2eRunId(null)
          return true
        }

        if (status.state === 'ERROR' || status.state === 'CANCELLED') {
          updateE2EStep(stepIdx, {
            status: 'error',
            output: `${runLabel} | ${status.state}: ${status.error || 'Unknown error'}`,
            duration_ms: elapsed,
          })
          setE2eRunId(null)
          setE2eRunning(false)
          setE2eActiveStep(null)
          setE2eRemainingSpecs([])
          return true
        }
      } catch { /* ignore */ }
      return false
    }

    poll().then((done) => { if (done) return })
    const interval = setInterval(async () => {
      const done = await poll()
      if (done) clearInterval(interval)
    }, 10000)
    return () => clearInterval(interval)
  }, [e2eRunId, e2eActiveStep])

  // Auto-continue: when a run completes, start next spec
  useEffect(() => {
    if (e2eRunId !== null || !e2eRunning || e2eRemainingSpecs.length === 0) return
    if (e2eCancelledRef.current) {
      setE2eRunning(false)
      setE2eRemainingSpecs([])
      return
    }
    const [next, ...rest] = e2eRemainingSpecs
    setE2eRemainingSpecs(rest)
    startE2ERunSpec(next)
  }, [e2eRunId, e2eRunning, e2eRemainingSpecs])

  // Main E2E handler
  async function handleRunE2E() {
    const steps = buildE2ESteps(e2eSF)
    setE2eSteps(steps)
    setE2eRunning(true)
    setRunCosts([null, null, null])
    setE2eRunId(null)
    setE2eActiveStep(null)
    setE2eRemainingSpecs([])
    e2eCancelledRef.current = false

    // Step 0: Check Google auth & verify/create prerequisites (VPC, subnet, NAT, GCS bucket)
    updateE2EStep(0, { status: 'running', output: 'Checking Google Cloud authentication and prerequisites...' })
    const step0Start = Date.now()
    try {
      const auth = await apiCall('/api/dataproc/auth/status')
      if (auth.status !== 'authenticated') {
        updateE2EStep(0, { status: 'error', output: `Not authenticated with Google Cloud: ${auth.message}. Please sign in first.`, duration_ms: Date.now() - step0Start })
        setE2eRunning(false)
        return
      }

      updateE2EStep(0, { output: `Authenticated as ${auth.account}. Checking & creating prerequisites...` })

      const prereqs = await apiCall('/api/dataproc/verify-prerequisites', {
        method: 'POST',
        body: JSON.stringify({
          project, region, subnet, gcs_bucket: gcsBucket,
          metastore_mode: metastoreMode,
          metastore_name: metastoreName,
          metastore_path: metastore,
        }),
      })

      const prereqDetails = (prereqs.steps || []).map((s: any) => ({
        label: s.step?.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase()),
        name: s.name || '',
        status: s.status || 'error',
        error: s.error,
      }))

      if (prereqs.status === 'ready') {
        updateE2EStep(0, {
          status: 'success',
          output: `Authenticated as ${auth.account}. All prerequisites ready.`,
          duration_ms: Date.now() - step0Start,
          details: prereqDetails,
        })
      } else {
        const errors = (prereqs.steps || []).filter((s: any) => s.status === 'error').map((s: any) => `${s.step}: ${s.error}`).join('; ')
        updateE2EStep(0, {
          status: 'error',
          output: `Prerequisites incomplete: ${errors}`,
          duration_ms: Date.now() - step0Start,
          details: prereqDetails,
        })
        setE2eRunning(false)
        return
      }
    } catch (e: any) {
      updateE2EStep(0, { status: 'error', output: e.message, duration_ms: Date.now() - step0Start })
      setE2eRunning(false)
      return
    }

    // Queue run specs: batch first, then incremental 2, then 3
    const [first, ...rest] = E2E_RUN_SPECS
    setE2eRemainingSpecs(rest)
    await startE2ERunSpec(first)
  }

  const [cancelling, setCancelling] = useState(false)

  async function handleCancelE2E() {
    if (!e2eRunId) return
    setCancelling(true)
    e2eCancelledRef.current = true
    try {
      await apiCall(`/api/dataproc/cancel-run/${e2eRunId}`, { method: 'POST' })
      addToast(`Cancellation requested for run ${e2eRunId}`, 'info')
      if (e2eActiveStep !== null) {
        updateE2EStep(e2eActiveStep, { output: `Cancellation requested for run ${e2eRunId}. Waiting...` })
      }
    } catch (e: any) {
      addToast(`Failed to cancel: ${e.message}`, 'error')
    } finally {
      setCancelling(false)
    }
  }

  // ── Manual mode handlers ─────────────────────────────────────────────

  async function handleManualRun() {
    setLoadingManualRun(true)
    try {
      const result = await apiCall('/api/dataproc/run-full-cycle', {
        method: 'POST',
        body: JSON.stringify({
          cluster_name: clusterName,
          project, region, zone, subnet,
          metastore: metastoreMode === 'existing' ? metastore : (metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : ''),
          image_version: '2.3-debian12',
          master_type: masterType,
          worker_type: workerType,
          num_workers: numWorkers,
          ssd_count: ssdCount,
          load_type: loadType,
          batch_id: batchId,
          scale_factor: parseInt(scaleFactor),
          gcs_bucket: gcsBucket,
          raw_data_path: rawDataPath,
          target_database: targetDatabase,
          metrics_output: metricsOutput,
        }),
      })
      setManualRunId(result.run_id)
      addToast(`Dataproc run started: ${result.run_id}`, 'success')
    } catch (e: any) {
      addToast(`Failed: ${e.message}`, 'error')
    } finally {
      setLoadingManualRun(false)
    }
  }

  // Poll manual run status
  useEffect(() => {
    if (!manualRunId) return
    const poll = async () => {
      try {
        const status = await apiCall(`/api/dataproc/run-status/${manualRunId}`)
        setManualRunStatus(status)
        if (status.state === 'DONE' || status.state === 'ERROR' || status.state === 'CANCELLED') {
          return true
        }
      } catch { /* ignore */ }
      return false
    }
    poll()
    const interval = setInterval(async () => {
      const done = await poll()
      if (done) clearInterval(interval)
    }, 10000)
    return () => clearInterval(interval)
  }, [manualRunId])

  // ── Render ───────────────────────────────────────────────────────────

  return (
    <div>
      {toasts.map((t) => (
        <Toast key={t.id} message={t.message} type={t.type} onClose={() => removeToast(t.id)} />
      ))}

      {/* Google Cloud Auth Banner */}
      <div className={`mb-4 p-4 rounded-lg border ${
        gcpAuth?.status === 'authenticated'
          ? 'bg-green-50 border-green-200'
          : 'bg-amber-50 border-amber-200'
      }`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm">
            <span className={`w-2.5 h-2.5 rounded-full ${gcpAuth?.status === 'authenticated' ? 'bg-green-500' : 'bg-amber-500'}`} />
            {gcpAuth?.status === 'authenticated' ? (
              <span className="text-green-700">
                Google Cloud: <span className="font-medium">{gcpAuth.account}</span>
                <span className="text-green-500 ml-1">({gcpAuth.method})</span>
              </span>
            ) : (
              <span className="text-amber-700">
                {gcpAuth?.message || 'Not authenticated with Google Cloud'}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {gcpAuth?.status === 'authenticated' ? (
              <button onClick={handleLogout} className="text-xs text-red-500 hover:text-red-700 underline">Sign out</button>
            ) : (
              <button
                onClick={() => setShowTokenInput(!showTokenInput)}
                className="px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
              >
                Sign in with Google
              </button>
            )}
            <button onClick={checkGcpAuth} disabled={loadingAuth} className="text-xs text-slate-500 underline hover:text-slate-700">
              {loadingAuth ? '...' : 'Refresh'}
            </button>
          </div>
        </div>

        {/* Token input form */}
        {showTokenInput && gcpAuth?.status !== 'authenticated' && (
          <div className="mt-4 pt-3 border-t border-amber-200">
            <div className="mb-3">
              <p className="text-xs text-slate-600 mb-2">
                Provide your Google Cloud credentials. The easiest way:
              </p>
              <ol className="text-xs text-slate-500 list-decimal ml-4 space-y-1">
                <li>Run <code className="bg-slate-100 px-1 rounded text-[11px]">cat ~/.config/gcloud/application_default_credentials.json</code> on your machine</li>
                <li>Select <strong>ADC JSON</strong> below and paste the full JSON content</li>
                <li>Or run <code className="bg-slate-100 px-1 rounded text-[11px]">gcloud auth print-access-token</code> and paste as <strong>Access Token</strong> (expires in 1hr)</li>
              </ol>
            </div>
            <div className="flex items-end gap-3">
              <div className="flex-shrink-0">
                <label className="block text-[10px] font-medium text-slate-500 uppercase mb-1">Token Type</label>
                <select
                  value={tokenType}
                  onChange={(e) => setTokenType(e.target.value as any)}
                  className="px-2 py-2 text-xs border border-slate-300 rounded-md bg-white"
                >
                  <option value="adc_json">ADC JSON</option>
                  <option value="refresh_token">Refresh Token</option>
                  <option value="access_token">Access Token</option>
                </select>
              </div>
              <div className="flex-1">
                <label className="block text-[10px] font-medium text-slate-500 uppercase mb-1">
                  {tokenType === 'adc_json' ? 'Paste full JSON' : tokenType === 'refresh_token' ? 'Refresh Token' : 'Access Token'}
                </label>
                <textarea
                  value={tokenInput}
                  onChange={(e) => setTokenInput(e.target.value)}
                  placeholder={tokenType === 'adc_json' ? '{"type":"authorized_user","client_id":"...","refresh_token":"..."}' : 'ya29.xxx...'}
                  rows={tokenType === 'adc_json' ? 3 : 1}
                  className="w-full px-3 py-2 text-xs font-mono border border-slate-300 rounded-md bg-white focus:ring-2 focus:ring-blue-400 focus:outline-none"
                />
              </div>
              <button
                onClick={handleTokenSubmit}
                disabled={loadingAuth || !tokenInput.trim()}
                className="px-4 py-2 text-xs font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 transition-colors flex-shrink-0"
              >
                {loadingAuth ? 'Authenticating...' : 'Authenticate'}
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Mode Toggle */}
      <div className="flex items-center gap-1 mb-6 p-1 bg-slate-100 rounded-lg w-fit">
        <button
          onClick={() => setMode('e2e')}
          className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
            mode === 'e2e'
              ? 'bg-white text-blue-700 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          End to End
        </button>
        <button
          onClick={() => setMode('manual')}
          className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
            mode === 'manual'
              ? 'bg-white text-blue-700 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          Manual
        </button>
      </div>

      {/* ── E2E Mode ────────────────────────────────────────────────── */}
      {mode === 'e2e' && (
        <Section title="End-to-End Dataproc Benchmark" defaultOpen={true} colorScheme="blue">
          <div className="pt-4">
            {/* Config Row */}
            <div className="flex flex-wrap items-end gap-4 mb-6">
              <FormField label="Scale Factor">
                <SelectInput
                  value={e2eSF}
                  onChange={setE2eSF}
                  options={[
                    { value: '100', label: 'SF 100' },
                    { value: '1000', label: 'SF 1000' },
                    { value: '10000', label: 'SF 10000' },
                  ]}
                />
              </FormField>
              <FormField label="GCP Project">
                <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700">{project}</div>
              </FormField>
              <FormField label="Region">
                <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700">{region}</div>
              </FormField>
              <FormField label="Raw Data Path">
                <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700 max-w-[260px] truncate" title={rawDataPath}>{rawDataPath}</div>
              </FormField>
              <FormField label="Hive Metastore">
                <SelectInput
                  value={metastoreMode}
                  onChange={(v) => setMetastoreMode(v as 'spark_default' | 'create' | 'existing')}
                  options={[
                    { value: 'spark_default', label: 'Spark Default' },
                    { value: 'create', label: 'Create Dataproc Metastore' },
                    { value: 'existing', label: 'Use Existing Metastore' },
                  ]}
                />
              </FormField>
              {metastoreMode === 'create' && (
                <FormField label="Metastore Name">
                  <TextInput value={metastoreName} onChange={setMetastoreName} placeholder="tpcdi-benchmark-metastore" />
                </FormField>
              )}
              {metastoreMode === 'existing' && (
                <FormField label="Metastore Path">
                  <TextInput value={metastore} onChange={setMetastore} placeholder="projects/.../locations/.../services/..." />
                </FormField>
              )}
              <ActionButton
                label={e2eRunning ? 'Running...' : 'Run Full Pipeline'}
                onClick={handleRunE2E}
                loading={e2eRunning}
                disabled={e2eRunning}
                variant="secondary"
              />
              {e2eRunning && e2eRunId && e2eRunId !== 'pending' && (
                <button
                  onClick={handleCancelE2E}
                  disabled={cancelling}
                  className="px-4 py-2 text-sm font-medium rounded-md border border-red-300 text-red-600 hover:bg-red-50 disabled:opacity-50 transition-all"
                >
                  {cancelling ? 'Cancelling...' : 'Cancel Run'}
                </button>
              )}
            </div>

            {/* Sizing preview */}
            <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
              <p className="text-xs font-semibold text-blue-700 mb-2">Pipeline: Batch → Incremental Batch 2 → Incremental Batch 3 (sequential, fail-fast)</p>
              <div className="flex flex-wrap gap-4 text-xs text-blue-600">
                {['batch', 'incremental'].map(lt => {
                  const sizing = getDataprocSizing(parseInt(e2eSF), lt)
                  return (
                    <div key={lt} className="bg-white/60 rounded px-2 py-1">
                      <span className="font-medium capitalize">{lt}:</span> master {sizing.master}, workers {sizing.worker} x{sizing.workers}, {sizing.ssds} SSDs
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Step Progress Cards */}
            <div className="space-y-3">
              {e2eSteps.map((step, i) => (
                <div
                  key={i}
                  className={`p-4 rounded-lg border transition-all ${
                    step.status === 'pending' ? 'bg-slate-50 border-slate-200' :
                    step.status === 'running' ? 'bg-blue-50 border-blue-300 shadow-sm' :
                    step.status === 'success' ? 'bg-green-50 border-green-300' :
                    'bg-red-50 border-red-300'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <span className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold ${
                        step.status === 'pending' ? 'bg-slate-200 text-slate-500' :
                        step.status === 'running' ? 'bg-blue-500 text-white animate-pulse' :
                        step.status === 'success' ? 'bg-green-500 text-white' :
                        'bg-red-500 text-white'
                      }`}>
                        {step.status === 'success' ? '\u2713' : step.status === 'error' ? '!' : i + 1}
                      </span>
                      <span className="text-sm font-semibold text-slate-700">{step.name}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      {step.duration_ms !== null && (
                        <span className="text-xs font-mono text-slate-500">{formatDuration(step.duration_ms)}</span>
                      )}
                      <span className={`text-[10px] font-medium uppercase px-2 py-0.5 rounded ${
                        step.status === 'pending' ? 'bg-slate-200 text-slate-500' :
                        step.status === 'running' ? 'bg-blue-100 text-blue-700' :
                        step.status === 'success' ? 'bg-green-100 text-green-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {step.status}
                      </span>
                    </div>
                  </div>
                  {step.output && (
                    <p className="mt-2 text-xs text-slate-600 ml-10 break-words">{step.output}</p>
                  )}
                  {step.link && (
                    <a href={step.link} target="_blank" rel="noopener noreferrer"
                      className="mt-1 ml-10 inline-block text-xs text-blue-600 hover:text-blue-700 underline">
                      View in GCP Console
                    </a>
                  )}
                  {/* Prereq resource details */}
                  {step.details && step.details.length > 0 && (
                    <div className="mt-3 ml-10 grid grid-cols-1 sm:grid-cols-3 gap-2">
                      {step.details.map((d, di) => (
                        <div key={di} className="flex items-start gap-2 p-2 rounded-md bg-white/70 border border-slate-100">
                          <span className={`mt-0.5 w-4 h-4 rounded-full flex items-center justify-center text-[9px] font-bold flex-shrink-0 ${
                            d.status === 'created' ? 'bg-blue-500 text-white' :
                            d.status === 'exists' ? 'bg-green-500 text-white' :
                            d.status === 'skipped' ? 'bg-slate-400 text-white' :
                            d.status === 'warning' ? 'bg-amber-500 text-white' :
                            d.status === 'creating' ? 'bg-blue-400 text-white animate-pulse' :
                            'bg-red-400 text-white'
                          }`}>
                            {d.status === 'exists' ? '\u2713' : d.status === 'created' ? '+' : d.status === 'skipped' ? '\u2013' : d.status === 'warning' ? '!' : d.status === 'creating' ? '\u2026' : '\u00d7'}
                          </span>
                          <div className="min-w-0">
                            <p className="text-[11px] font-medium text-slate-700 capitalize">{d.label}</p>
                            <p className="text-[10px] text-slate-500 font-mono truncate" title={d.name}>{d.name}</p>
                            {d.status && d.status !== 'exists' && (
                              <span className={`inline-block mt-0.5 text-[9px] font-medium px-1.5 py-0.5 rounded ${
                                d.status === 'created' ? 'bg-blue-100 text-blue-700' :
                                d.status === 'error' ? 'bg-red-100 text-red-700' :
                                d.status === 'skipped' ? 'bg-slate-100 text-slate-600' :
                                d.status === 'warning' ? 'bg-amber-100 text-amber-700' :
                                d.status === 'creating' ? 'bg-blue-100 text-blue-600' :
                                'bg-slate-100 text-slate-600'
                              }`}>{d.status}</span>
                            )}
                            {d.error && <p className="text-[9px] text-red-500 truncate" title={d.error}>{d.error}</p>}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Cost Breakdown */}
            {runCosts.some(c => c !== null) && (
              <div className="mt-6 space-y-4">
                {(() => {
                  const totalCost = runCosts.reduce((sum, c) => sum + (c?.total_cost_usd || 0), 0)
                  const totalDuration = runCosts.reduce((sum, c) => sum + (c?.duration_ms || 0), 0)
                  return (
                    <div className="p-4 bg-gradient-to-r from-blue-50 to-cyan-50 rounded-lg border border-blue-200">
                      <div className="flex items-center justify-between">
                        <h4 className="text-sm font-bold text-blue-800">Pipeline Total (Dataproc)</h4>
                        <div className="flex items-center gap-6">
                          <div className="text-right">
                            <p className="text-[10px] text-blue-600 uppercase">Total Cost</p>
                            <p className="text-xl font-bold text-blue-700">{formatCost(totalCost)}</p>
                          </div>
                          <div className="text-right">
                            <p className="text-[10px] text-blue-600 uppercase">Total Duration</p>
                            <p className="text-xl font-bold text-blue-700">{formatDuration(totalDuration)}</p>
                          </div>
                          {totalDuration > 0 && (
                            <div className="text-right">
                              <p className="text-[10px] text-blue-600 uppercase">Avg $/hr</p>
                              <p className="text-xl font-bold text-blue-700">
                                ${(totalCost / (totalDuration / 3_600_000)).toFixed(2)}
                              </p>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  )
                })()}
                {runCosts.map((cost, ci) => {
                  if (!cost) return null
                  const labels = ['Batch', 'Incremental Batch 2', 'Incremental Batch 3']
                  return (
                    <div key={ci} className="p-4 bg-gradient-to-r from-slate-50 to-blue-50 rounded-lg border border-slate-200">
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-sm font-bold text-slate-700">{labels[ci]} (SF {e2eSF})</h4>
                        {cost.duration_hours > 0 && (
                          <span className="text-[10px] text-slate-400 font-mono">
                            ${(cost.total_cost_usd / cost.duration_hours).toFixed(2)}/hr
                          </span>
                        )}
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">VM Compute</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.vm_cost_usd)}</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">Dataproc Premium</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.dataproc_premium_usd)}</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">Local SSD</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.ssd_cost_usd)}</p>
                          <p className="text-[9px] text-slate-400">{cost.cluster.ssd_count} SSD(s)</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-blue-200 bg-blue-50">
                          <p className="text-[10px] text-blue-600 uppercase font-semibold">Total</p>
                          <p className="text-base font-bold text-blue-700">{formatCost(cost.total_cost_usd)}</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">Duration</p>
                          <p className="text-sm font-mono text-slate-700">{formatDuration(cost.duration_ms)}</p>
                          <p className="text-[9px] text-slate-400">{cost.cluster.worker_type} x{cost.cluster.num_workers}</p>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </Section>
      )}

      {/* ── Manual Mode ─────────────────────────────────────────────── */}
      {mode === 'manual' && (
        <>
          <Section title="Dataproc Cluster & Job Configuration" defaultOpen={true} colorScheme="blue">
            <div className="pt-4">
              <div className="flex items-center gap-2 mb-5 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                <svg className="w-5 h-5 text-blue-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <p className="text-sm text-blue-700">
                  Full cycle: Create Cluster → Submit PySpark Job → Wait → Delete Cluster. Uses your gcloud auth.
                </p>
              </div>

              {/* GCP Config */}
              <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">GCP & Network</h4>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                <FormField label="GCP Project">
                  <TextInput value={project} onChange={setProject} placeholder="my-project" />
                </FormField>
                <FormField label="Region">
                  <SelectInput
                    value={region}
                    onChange={setRegion}
                    options={[
                      { value: 'us-central1', label: 'us-central1' },
                      { value: 'us-east1', label: 'us-east1' },
                      { value: 'us-west1', label: 'us-west1' },
                      { value: 'europe-west1', label: 'europe-west1' },
                      { value: 'asia-east1', label: 'asia-east1' },
                    ]}
                  />
                </FormField>
                <FormField label="Zone">
                  <TextInput value={zone} onChange={setZone} placeholder="us-central1-a" />
                </FormField>
                <FormField label="Subnet">
                  <TextInput value={subnet} onChange={setSubnet} placeholder="subnet-name" />
                </FormField>
                <FormField label="GCS Bucket">
                  <TextInput value={gcsBucket} onChange={setGcsBucket} placeholder="my-bucket" />
                </FormField>
                <FormField label="Hive Metastore">
                  <SelectInput
                    value={metastoreMode}
                    onChange={(v) => setMetastoreMode(v as 'spark_default' | 'create' | 'existing')}
                    options={[
                      { value: 'spark_default', label: 'Spark Default (no external metastore)' },
                      { value: 'create', label: 'Create Dataproc Metastore' },
                      { value: 'existing', label: 'Use Existing Metastore' },
                    ]}
                  />
                </FormField>
                {metastoreMode === 'create' && (
                  <FormField label="Metastore Name">
                    <TextInput value={metastoreName} onChange={setMetastoreName} placeholder="tpcdi-benchmark-metastore" />
                  </FormField>
                )}
                {metastoreMode === 'existing' && (
                  <FormField label="Metastore Service Path">
                    <TextInput value={metastore} onChange={setMetastore} placeholder="projects/.../locations/.../services/..." />
                  </FormField>
                )}
              </div>

              {/* Cluster Config */}
              <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">Cluster</h4>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                <FormField label="Cluster Name">
                  <TextInput value={clusterName} onChange={setClusterName} placeholder="tpcdi-cluster" />
                </FormField>
                <FormField label="Master Machine Type">
                  <SelectInput value={masterType} onChange={setMasterType} options={gcpNodeTypes} />
                </FormField>
                <FormField label="Worker Machine Type">
                  <SelectInput value={workerType} onChange={setWorkerType} options={gcpNodeTypes} />
                </FormField>
                <FormField label="Number of Workers">
                  <NumberInput value={numWorkers} onChange={setNumWorkers} min={1} max={100} />
                </FormField>
                <FormField label="Worker SSDs">
                  <NumberInput value={ssdCount} onChange={setSsdCount} min={0} max={8} />
                </FormField>
              </div>

              {/* Job Config */}
              <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">Job</h4>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                <FormField label="Load Type">
                  <SelectInput
                    value={loadType}
                    onChange={setLoadType}
                    options={[
                      { value: 'batch', label: 'Batch' },
                      { value: 'incremental', label: 'Incremental' },
                    ]}
                  />
                </FormField>
                <FormField label="Scale Factor">
                  <SelectInput
                    value={scaleFactor}
                    onChange={setScaleFactor}
                    options={[
                      { value: '100', label: 'SF 100' },
                      { value: '1000', label: 'SF 1000' },
                      { value: '10000', label: 'SF 10000' },
                    ]}
                  />
                </FormField>
                {loadType === 'incremental' && (
                  <FormField label="Batch ID">
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
                <FormField label="Raw Data Path">
                  <TextInput value={rawDataPath} onChange={setRawDataPath} placeholder="gs://bucket/tpcdi" />
                </FormField>
                <FormField label="Target Database">
                  <TextInput value={targetDatabase} onChange={setTargetDatabase} placeholder="tpcdi_warehouse" />
                </FormField>
                <FormField label="Metrics Output">
                  <TextInput value={metricsOutput} onChange={setMetricsOutput} placeholder="gs://bucket/metrics" />
                </FormField>
              </div>

              <div className="mt-5 pt-4 border-t border-slate-100">
                <ActionButton
                  label={loadingManualRun ? 'Starting...' : 'Run Full Cycle'}
                  onClick={handleManualRun}
                  loading={loadingManualRun}
                  variant="secondary"
                />
              </div>
            </div>
          </Section>

          {/* Manual Run Status */}
          {manualRunStatus && (
            <Section title="Run Status" defaultOpen={true} colorScheme="slate">
              <div className="pt-4">
                <div className={`p-4 rounded-lg border ${
                  manualRunStatus.state === 'DONE' ? 'bg-green-50 border-green-300' :
                  manualRunStatus.state === 'ERROR' ? 'bg-red-50 border-red-300' :
                  'bg-blue-50 border-blue-300'
                }`}>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                    <div>
                      <p className="text-[10px] text-slate-500 uppercase">Run ID</p>
                      <p className="font-mono font-medium text-slate-700">{manualRunStatus.run_id}</p>
                    </div>
                    <div>
                      <p className="text-[10px] text-slate-500 uppercase">State</p>
                      <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                        manualRunStatus.state === 'DONE' ? 'bg-green-100 text-green-700' :
                        manualRunStatus.state === 'ERROR' ? 'bg-red-100 text-red-700' :
                        manualRunStatus.state === 'CANCELLED' ? 'bg-yellow-100 text-yellow-700' :
                        'bg-blue-100 text-blue-700'
                      }`}>
                        {manualRunStatus.state}
                      </span>
                    </div>
                    <div>
                      <p className="text-[10px] text-slate-500 uppercase">Phase</p>
                      <p className="font-medium text-slate-700 capitalize">{manualRunStatus.phase.replace(/_/g, ' ')}</p>
                    </div>
                    <div>
                      <p className="text-[10px] text-slate-500 uppercase">Duration</p>
                      <p className="font-mono font-medium text-slate-700">{formatDuration(manualRunStatus.duration_ms)}</p>
                    </div>
                  </div>
                  {manualRunStatus.error && (
                    <p className="mt-3 text-xs text-red-600">{manualRunStatus.error}</p>
                  )}
                </div>

                {/* Manual cost display */}
                {manualRunStatus.cost && (
                  <div className="mt-4 p-4 bg-gradient-to-r from-slate-50 to-blue-50 rounded-lg border border-slate-200">
                    <h4 className="text-sm font-bold text-slate-700 mb-3">Cost Breakdown</h4>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">VM Compute</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(manualRunStatus.cost.vm_cost_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">Dataproc Premium</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(manualRunStatus.cost.dataproc_premium_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">Local SSD</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(manualRunStatus.cost.ssd_cost_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-blue-200 bg-blue-50">
                        <p className="text-[10px] text-blue-600 uppercase font-semibold">Total</p>
                        <p className="text-base font-bold text-blue-700">{formatCost(manualRunStatus.cost.total_cost_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">Duration</p>
                        <p className="text-sm font-mono text-slate-700">{formatDuration(manualRunStatus.cost.duration_ms)}</p>
                        <p className="text-[9px] text-slate-400">{manualRunStatus.cost.cluster.worker_type} x{manualRunStatus.cost.cluster.num_workers}</p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </Section>
          )}
        </>
      )}
    </div>
  )
}
