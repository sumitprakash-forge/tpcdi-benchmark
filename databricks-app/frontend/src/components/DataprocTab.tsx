import { useState, useEffect, useCallback, useRef } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput } from './FormField'
import ComputeModeSelector, { type ComputeMode } from './ComputeModeSelector'
import Toast from './Toast'
import RunHistorySection from './RunHistorySection'

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
  compute_mode?: string
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
  lockedComputeMode?: string
  lockedServerlessTier?: string
}

const E2E_STATE_KEY = 'tpcdi_dataproc_e2e_state'

const E2E_RUN_SPECS: E2ERunSpec[] = [
  { loadType: 'batch', batchId: '', stepIndex: 1 },
  { loadType: 'incremental', batchId: '2', stepIndex: 2 },
  { loadType: 'incremental', batchId: '3', stepIndex: 3 },
]

const MANUAL_PIPELINE_SPECS: { loadType: string; batchId: string; stepIndex: number; label: string; clusterSuffix: string }[] = [
  { loadType: 'batch', batchId: '', stepIndex: 0, label: 'Batch Load', clusterSuffix: '-batch' },
  { loadType: 'incremental', batchId: '2', stepIndex: 1, label: 'Incremental Load - Batch 2', clusterSuffix: '-inc-b2' },
  { loadType: 'incremental', batchId: '3', stepIndex: 2, label: 'Incremental Load - Batch 3', clusterSuffix: '-inc-b3' },
]

interface ManualPipelineStep {
  name: string
  status: 'pending' | 'running' | 'success' | 'error'
  output: string
  duration_ms: number | null
  link?: string
}

function buildManualPipelineSteps(): ManualPipelineStep[] {
  return MANUAL_PIPELINE_SPECS.map(spec => ({
    name: spec.label,
    status: 'pending' as const,
    output: '',
    duration_ms: null,
  }))
}

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
  const [computeMode, setComputeMode] = useState<ComputeMode>('classic')
  const computeModeRef = useRef(computeMode)
  computeModeRef.current = computeMode
  const [serverlessTier, setServerlessTier] = useState<'standard' | 'premium'>('standard')
  const serverlessTierRef = useRef(serverlessTier)
  serverlessTierRef.current = serverlessTier

  // Common config
  const [project, setProject] = useState('gcp-sandbox-field-eng')
  const [region, setRegion] = useState('us-central1')
  const [zone, setZone] = useState('us-central1-a')
  const [subnet, setSubnet] = useState('tpcdi-benchmark-subnet')
  const [metastoreMode] = useState<'spark_default' | 'create' | 'existing'>('create')
  const [metastoreName, setMetastoreName] = useState('tpcdi-benchmark-metastore')
  const [gcsBucket, setGcsBucket] = useState('sumit_prakash_gcs')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [targetDatabase, setTargetDatabase] = useState('tpcdi_warehouse')
  const [metricsOutput, setMetricsOutput] = useState('gs://sumit_prakash_gcs/tpcdi/metrics')

  // Manual mode config
  const [scaleFactor, setScaleFactor] = useState('1000')

  // Per-step cluster config (batch, inc-b2, inc-b3)
  interface StepClusterConfig {
    clusterName: string
    masterType: string
    workerType: string
    numWorkers: number
    ssdCount: number
  }
  function buildDefaultStepConfigs(sf: number): StepClusterConfig[] {
    return MANUAL_PIPELINE_SPECS.map(spec => {
      const sizing = getDataprocSizing(sf, spec.loadType)
      return {
        clusterName: `tpcdi-dp-sf${sf}${spec.clusterSuffix}`,
        masterType: sizing.master, workerType: sizing.worker,
        numWorkers: sizing.workers, ssdCount: sizing.ssds,
      }
    })
  }
  const [stepConfigs, setStepConfigs] = useState<StepClusterConfig[]>(buildDefaultStepConfigs(1000))
  const [expandedStepConfig, setExpandedStepConfig] = useState<number | null>(null)

  // Teardown state per step
  const [teardownStatus, setTeardownStatus] = useState<('idle' | 'deleting' | 'deleted' | 'not_found' | 'error')[]>(['idle', 'idle', 'idle'])

  function updateStepConfig(idx: number, updates: Partial<StepClusterConfig>) {
    setStepConfigs(prev => prev.map((c, i) => i === idx ? { ...c, ...updates } : c))
  }

  // Manual prereqs state
  interface PrereqCheckItem {
    label: string
    name: string
    status: 'pending' | 'checking' | 'exists' | 'created' | 'creating' | 'skipped' | 'warning' | 'error'
    error?: string
    message?: string
  }
  const [manualPrereqs, setManualPrereqs] = useState<PrereqCheckItem[]>([])
  const [manualPrereqStatus, setManualPrereqStatus] = useState<'idle' | 'checking' | 'ready' | 'error'>('idle')
  const [manualPrereqChecking, setManualPrereqChecking] = useState(false)

  // Auto-update per-step configs when scale factor changes
  useEffect(() => {
    const sf = parseInt(scaleFactor) || 1000
    setStepConfigs(buildDefaultStepConfigs(sf))
    setTeardownStatus(['idle', 'idle', 'idle'])
  }, [scaleFactor])

  // Manual pipeline state
  const [manualPipelineSteps, setManualPipelineSteps] = useState<ManualPipelineStep[]>(buildManualPipelineSteps())
  const [manualPipelineRunning, setManualPipelineRunning] = useState(false)
  const [manualActiveStep, setManualActiveStep] = useState<number | null>(null)
  const [manualCosts, setManualCosts] = useState<(DataprocRunCost | null)[]>([null, null, null])
  const [manualRunId, setManualRunId] = useState<string | null>(null)
  const [manualRemainingSpecs, setManualRemainingSpecs] = useState<typeof MANUAL_PIPELINE_SPECS>([])
  const manualCancelledRef = useRef(false)

  // Google auth state
  const [_gcpAuth, setGcpAuth] = useState<{ status: string; method?: string; account?: string; message?: string } | null>(null)
  const [_loadingAuth, setLoadingAuth] = useState(false)


  // Mandatory GCP refresh token (persisted in localStorage, auto-refreshes before every run)
  const [gcpRefreshToken, setGcpRefreshToken] = useState(() => localStorage.getItem('tpcdi_gcp_refresh_token') || '')
  const [gcpTokenUser, setGcpTokenUser] = useState<string | null>(null)
  const [gcpTokenValidating, setGcpTokenValidating] = useState(false)
  const [gcpTokenError, setGcpTokenError] = useState<string | null>(null)

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
  const [e2eSaveNeeded, setE2eSaveNeeded] = useState(false)

  // Persist E2E state
  useEffect(() => {
    const state: E2EPersistedState = {
      steps: e2eSteps, sf: e2eSF, runId: e2eRunId, activeStep: e2eActiveStep,
      running: e2eRunning, costs: runCosts, remainingSpecs: e2eRemainingSpecs,
      lockedComputeMode: e2eLockedComputeMode.current,
      lockedServerlessTier: e2eLockedServerlessTier.current,
    }
    localStorage.setItem(E2E_STATE_KEY, JSON.stringify(state))
  }, [e2eSteps, e2eSF, e2eRunId, e2eActiveStep, e2eRunning, runCosts, e2eRemainingSpecs])

  // On mount: check backend for active runs and reconnect, or reset stale state
  useEffect(() => {
    if (!e2eRunning && !e2eRunId) return // nothing to reconnect
    let cancelled = false
    ;(async () => {
      try {
        const res = await fetch('/api/dataproc/active-runs')
        if (!res.ok || cancelled) return
        const activeRuns: DataprocRunStatus[] = await res.json()

        if (e2eRunId && activeRuns.some(r => r.run_id === e2eRunId)) {
          // Server still has our run — resume polling (keep state as-is)
          return
        }

        // Check if any active run matches our persisted config (reconnect by config)
        const savedStep = e2eActiveStep
        if (savedStep !== null && activeRuns.length > 0) {
          const spec = E2E_RUN_SPECS.find(s => s.stepIndex === savedStep)
          const match = spec && activeRuns.find(r =>
            r.config?.load_type === spec.loadType && r.config?.batch_id === spec.batchId && r.state === 'RUNNING'
          )
          if (match) {
            // Found a matching active run — reconnect
            setE2eRunId(match.run_id)
            setE2eRunning(true)
            return
          }
        }

        // No active runs on server — reset stale state
        if (e2eRunning) setE2eRunning(false)
        if (e2eRunId) setE2eRunId(null)
      } catch {
        // Server unreachable — reset to avoid stuck spinner
        if (e2eRunning) setE2eRunning(false)
        if (e2eRunId) setE2eRunId(null)
      }
    })()
    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // Only on mount

  // Auto-save Dataproc E2E run history when pipeline completes (uses fresh state)
  useEffect(() => {
    if (!e2eSaveNeeded) return
    setE2eSaveNeeded(false)
    const hasSuccess = e2eSteps.some(s => s.status === 'success')
    if (!hasSuccess) return
    fetch('/api/databricks/save-run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        run_data: {
          platform: 'dataproc',
          mode: 'e2e',
          flow_type: 'e2e',
          compute_mode: e2eLockedComputeMode.current,
          serverless_tier: e2eLockedComputeMode.current === 'serverless' ? e2eLockedServerlessTier.current : undefined,
          scale_factor: e2eSF,
          steps: e2eSteps,
          costs: runCosts,
          completed_at: new Date().toISOString(),
          status: e2eSteps.every(s => s.status === 'success') ? 'completed' : 'failed',
        }
      })
    }).catch(() => {})
  }, [e2eSaveNeeded, e2eSteps, e2eSF, runCosts, computeMode, serverlessTier])

  // Loading / Toast
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

  async function handleLogout() {
    await apiCall('/api/dataproc/auth/logout', { method: 'POST' })
    setGcpAuth({ status: 'unauthenticated', message: 'Logged out' })
    addToast('Google Cloud credentials cleared', 'info')
  }

  // ── GCP Refresh Token validation (debounced) ────────────────────────
  useEffect(() => {
    localStorage.setItem('tpcdi_gcp_refresh_token', gcpRefreshToken)
    if (!gcpRefreshToken.trim()) {
      setGcpTokenUser(null)
      setGcpTokenError(null)
      return
    }
    setGcpTokenValidating(true)
    setGcpTokenError(null)
    setGcpTokenUser(null)
    const timer = setTimeout(async () => {
      try {
        const data = await apiCall('/api/dataproc/auth/login', {
          method: 'POST',
          body: JSON.stringify({ token: gcpRefreshToken.trim(), token_type: 'refresh_token' }),
        })
        if (data.status === 'authenticated') {
          setGcpTokenUser(data.account || 'unknown')
          setGcpAuth(data)
        } else {
          setGcpTokenError(data.message || 'Invalid token')
        }
      } catch (e: any) {
        setGcpTokenError(e.message || 'Invalid token')
      } finally {
        setGcpTokenValidating(false)
      }
    }, 800)
    return () => clearTimeout(timer)
  }, [gcpRefreshToken])

  // ── E2E helpers ──────────────────────────────────────────────────────

  function updateE2EStep(index: number, updates: Partial<E2EStep>) {
    setE2eSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s))
  }

  const e2eCancelledRef = useRef(false)
  // Lock compute mode at pipeline start so UI changes don't affect running pipeline
  // Restored from localStorage on mount if pipeline was running
  const e2eLockedComputeMode = useRef<ComputeMode>(
    ((() => { try { const r = localStorage.getItem(E2E_STATE_KEY); return r ? JSON.parse(r).lockedComputeMode : null } catch { return null } })() as ComputeMode) || 'classic'
  )
  const e2eLockedServerlessTier = useRef<'standard' | 'premium'>(
    ((() => { try { const r = localStorage.getItem(E2E_STATE_KEY); return r ? JSON.parse(r).lockedServerlessTier : null } catch { return null } })() as 'standard' | 'premium') || 'standard'
  )
  const [e2eNextSpec, setE2eNextSpec] = useState<E2ERunSpec | null>(null)

  function buildRunLabel(spec: E2ERunSpec, sf: number): string {
    const isServerless = e2eLockedComputeMode.current === 'serverless'
    const tierLabel = e2eLockedServerlessTier.current === 'premium' ? 'Premium' : 'Standard'
    const clusterInfo = isServerless
      ? `Serverless ${tierLabel}`
      : (() => { const s = getDataprocSizing(sf, spec.loadType); return `${s.worker} x${s.workers}, ${s.ssds} SSDs` })()
    return spec.loadType === 'batch'
      ? `SF ${sf} | Batch | ${clusterInfo}`
      : `SF ${sf} | Incremental Batch ${spec.batchId} | ${clusterInfo}`
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
          metastore: metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : '',
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
          gcp_refresh_token: gcpRefreshToken.trim(),
          compute_mode: e2eLockedComputeMode.current,
          serverless_tier: e2eLockedServerlessTier.current,
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
    let failCount = 0
    let stepDone = false  // prevent double-trigger from concurrent polls
    const stepIdx = e2eActiveStep
    const sf = parseInt(e2eSF)
    const spec = E2E_RUN_SPECS.find(s => s.stepIndex === stepIdx)
    const runLabel = spec ? buildRunLabel(spec, sf) : `Step ${stepIdx}`

    const phaseIcon = (phase: string) => {
      if (phase.includes('creating')) return 'Cluster provisioning'
      if (phase.includes('submitting_batch')) return 'Submitting batch'
      if (phase.includes('running_job')) return 'Job executing'
      if (phase.includes('deleting')) return 'Cluster teardown'
      if (phase === 'completed') return 'Complete'
      if (phase === 'failed') return 'Failed'
      return phase.replace(/_/g, ' ')
    }

    const poll = async () => {
      try {
        const status: DataprocRunStatus = await apiCall(`/api/dataproc/run-status/${e2eRunId}`)

        // Sync locked compute mode from backend (handles new browser / incognito)
        if (status.compute_mode) {
          e2eLockedComputeMode.current = status.compute_mode as ComputeMode
        }

        const elapsed = status.start_time_ms
          ? Date.now() - status.start_time_ms
          : null

        // Rebuild run label with synced compute mode
        const currentRunLabel = spec ? buildRunLabel(spec, sf) : `Step ${stepIdx}`
        const parts: string[] = [currentRunLabel]
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

        // Build GCP Console link — batches for serverless, jobs for classic
        const isServerless = status.compute_mode === 'serverless' || computeMode === 'serverless'
        const gcpLink = status.job_id
          ? isServerless
            ? `https://console.cloud.google.com/dataproc/batches/${region}/${status.job_id}?project=${project}`
            : `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
          : status.cluster_name && !isServerless
            ? `https://console.cloud.google.com/dataproc/clusters/${status.cluster_name}?project=${project}&region=${region}`
            : undefined

        updateE2EStep(stepIdx, {
          output: parts.join(' | '),
          duration_ms: elapsed,
          link: gcpLink,
        })

        if (status.state === 'DONE') {
          if (stepDone) return true  // already handled by concurrent poll
          stepDone = true
          // Build completion summary with timing breakdown
          const timingParts: string[] = [currentRunLabel, 'Done']
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
            ? isServerless
              ? `https://console.cloud.google.com/dataproc/batches/${region}/${status.job_id}?project=${project}`
              : `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
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
          // Signal auto-continue by popping next spec
          setE2eRemainingSpecs(prev => {
            if (e2eCancelledRef.current) {
              setE2eRunning(false)
              return []
            }
            if (prev.length === 0) {
              setE2eRunning(false)
              setE2eActiveStep(null)
              setE2eSaveNeeded(true)
              return prev
            }
            const [next, ...rest] = prev
            setE2eNextSpec(next)
            return rest
          })
          return true
        }

        if (status.state === 'ERROR' || status.state === 'CANCELLED') {
          if (stepDone) return true
          stepDone = true
          updateE2EStep(stepIdx, {
            status: 'error',
            output: `${currentRunLabel} | ${status.state}: ${status.error || 'Unknown error'}`,
            duration_ms: elapsed,
          })
          setE2eRunId(null)
          setE2eRunning(false)
          setE2eActiveStep(null)
          setE2eRemainingSpecs([])
          return true
        }
      } catch (e) {
        failCount++
        if (failCount >= 5) {
          updateE2EStep(stepIdx, {
            status: 'error',
            output: `${runLabel} | Lost connection to server after ${failCount} retries`,
          })
          setE2eRunId(null)
          setE2eRunning(false)
          setE2eActiveStep(null)
          setE2eRemainingSpecs([])
          return true
        }
        updateE2EStep(stepIdx, {
          output: `${runLabel} | Reconnecting... (attempt ${failCount}/5)`,
        })
      }
      return false
    }

    poll().then((done) => { if (done) return })
    const interval = setInterval(async () => {
      const done = await poll()
      if (done) clearInterval(interval)
    }, 10000)

    // Resume polling immediately when tab becomes visible again
    const onVisible = () => {
      if (document.visibilityState === 'visible') poll()
    }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      clearInterval(interval)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [e2eRunId, e2eActiveStep])

  // Auto-continue: when e2eNextSpec is set, start the next run
  useEffect(() => {
    if (!e2eNextSpec) return
    const spec = e2eNextSpec
    setE2eNextSpec(null)
    startE2ERunSpec(spec)
  }, [e2eNextSpec])

  // Main E2E handler
  async function handleRunE2E() {
    // Lock compute mode for the entire pipeline run — UI changes won't affect it
    e2eLockedComputeMode.current = computeMode
    e2eLockedServerlessTier.current = serverlessTier

    const steps = buildE2ESteps(e2eSF)
    setE2eSteps(steps)
    setE2eRunning(true)
    setRunCosts([null, null, null])
    setE2eRunId(null)
    setE2eActiveStep(null)
    setE2eRemainingSpecs([])
    e2eCancelledRef.current = false

    // Step 0: Check Google auth & verify/create prerequisites (VPC, subnet, NAT, GCS bucket)
    updateE2EStep(0, { status: 'running', output: 'Refreshing Google Cloud credentials and checking prerequisites...' })
    const step0Start = Date.now()
    try {
      // Auto-refresh: if refresh token is provided, re-authenticate first
      let authAccount = ''
      if (gcpRefreshToken.trim()) {
        const refreshResult = await apiCall('/api/dataproc/auth/login', {
          method: 'POST',
          body: JSON.stringify({ token: gcpRefreshToken.trim(), token_type: 'refresh_token' }),
        })
        if (refreshResult.status !== 'authenticated') {
          updateE2EStep(0, { status: 'error', output: `GCP token refresh failed: ${refreshResult.message}`, duration_ms: Date.now() - step0Start })
          setE2eRunning(false)
          return
        }
        setGcpAuth(refreshResult)
        authAccount = refreshResult.account || ''
        updateE2EStep(0, { output: `Authenticated as ${authAccount}. Checking & creating prerequisites...` })
      } else {
        const auth = await apiCall('/api/dataproc/auth/status')
        if (auth.status !== 'authenticated') {
          updateE2EStep(0, { status: 'error', output: `Not authenticated with Google Cloud: ${auth.message}. Please provide a GCP Refresh Token.`, duration_ms: Date.now() - step0Start })
          setE2eRunning(false)
          return
        }
        authAccount = auth.account || ''
        updateE2EStep(0, { output: `Authenticated as ${authAccount}. Checking & creating prerequisites...` })
      }

      const prereqs = await apiCall('/api/dataproc/verify-prerequisites', {
        method: 'POST',
        body: JSON.stringify({
          project, region, subnet, gcs_bucket: gcsBucket,
          metastore_mode: metastoreMode,
          metastore_name: metastoreName,
          metastore_path: metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : '',
          gcp_refresh_token: gcpRefreshToken.trim(),
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
          output: `Authenticated as ${authAccount}. All prerequisites ready.`,
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

  // ── Manual pipeline handlers ─────────────────────────────────────────

  function updateManualStep(index: number, updates: Partial<ManualPipelineStep>) {
    setManualPipelineSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s))
  }

  async function startManualPipelineStep(spec: typeof MANUAL_PIPELINE_SPECS[0]) {
    const { loadType: lt, batchId: bid, stepIndex, label } = spec
    const cfg = stepConfigs[stepIndex]

    setManualRunId('pending')
    setManualActiveStep(stepIndex)
    updateManualStep(stepIndex, { status: 'running', output: `${label} | ${cfg.clusterName} | ${cfg.workerType} x${cfg.numWorkers}, ${cfg.ssdCount} SSDs | Starting full cycle...` })

    try {
      const result = await apiCall('/api/dataproc/run-full-cycle', {
        method: 'POST',
        body: JSON.stringify({
          cluster_name: cfg.clusterName,
          project, region, zone, subnet,
          metastore: metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : '',
          image_version: '2.3-debian12',
          master_type: cfg.masterType,
          worker_type: cfg.workerType,
          num_workers: cfg.numWorkers,
          ssd_count: cfg.ssdCount,
          load_type: lt,
          batch_id: bid,
          scale_factor: parseInt(scaleFactor),
          gcs_bucket: gcsBucket,
          raw_data_path: rawDataPath,
          target_database: targetDatabase,
          metrics_output: metricsOutput,
          gcp_refresh_token: gcpRefreshToken.trim(),
          compute_mode: computeMode,
          serverless_tier: serverlessTier,
        }),
      })
      setManualRunId(result.run_id)
      updateManualStep(stepIndex, { output: `${label} | Run ${result.run_id} started. Polling...` })
    } catch (e: any) {
      setManualRunId(null)
      updateManualStep(stepIndex, { status: 'error', output: `${label} | ${e.message}` })
      setManualPipelineRunning(false)
      setManualRemainingSpecs([])
    }
  }

  // Polling useEffect for manual pipeline run
  useEffect(() => {
    if (!manualRunId || manualRunId === 'pending' || manualActiveStep === null) return
    let failCount = 0
    const stepIdx = manualActiveStep
    const spec = MANUAL_PIPELINE_SPECS[stepIdx]
    const label = spec?.label || `Step ${stepIdx + 1}`

    const phaseIcon = (phase: string) => {
      if (phase.includes('creating')) return 'Cluster provisioning'
      if (phase.includes('submitting_batch')) return 'Submitting batch'
      if (phase.includes('running_job')) return 'Job executing'
      if (phase.includes('deleting')) return 'Cluster teardown'
      if (phase === 'completed') return 'Complete'
      if (phase === 'failed') return 'Failed'
      return phase.replace(/_/g, ' ')
    }

    const poll = async () => {
      try {
        const status: DataprocRunStatus = await apiCall(`/api/dataproc/run-status/${manualRunId}`)
        const elapsed = status.start_time_ms
          ? Date.now() - status.start_time_ms
          : null

        const parts: string[] = [label]
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

        const isServerlessManual = status.compute_mode === 'serverless' || computeMode === 'serverless'
        const gcpLink = status.job_id
          ? isServerlessManual
            ? `https://console.cloud.google.com/dataproc/batches/${region}/${status.job_id}?project=${project}`
            : `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
          : status.cluster_name && !isServerlessManual
            ? `https://console.cloud.google.com/dataproc/clusters/${status.cluster_name}?project=${project}&region=${region}`
            : undefined

        updateManualStep(stepIdx, {
          output: parts.join(' | '),
          duration_ms: elapsed,
          link: gcpLink,
        })

        if (status.state === 'DONE') {
          const timingParts: string[] = [label, 'Done']
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
            ? isServerlessManual
              ? `https://console.cloud.google.com/dataproc/batches/${region}/${status.job_id}?project=${project}`
              : `https://console.cloud.google.com/dataproc/jobs/${status.job_id}?project=${project}&region=${region}`
            : undefined

          updateManualStep(stepIdx, {
            status: 'success',
            output: timingParts.join(' | '),
            duration_ms: status.duration_ms,
            link: doneLink,
          })
          if (status.cost) {
            setManualCosts(prev => {
              const next = [...prev]
              next[stepIdx] = status.cost
              return next
            })
          }
          setManualRunId(null)
          return true
        }

        if (status.state === 'ERROR' || status.state === 'CANCELLED') {
          updateManualStep(stepIdx, {
            status: 'error',
            output: `${label} | ${status.state}: ${status.error || 'Unknown error'}`,
            duration_ms: elapsed,
          })
          setManualRunId(null)
          setManualPipelineRunning(false)
          setManualActiveStep(null)
          setManualRemainingSpecs([])
          return true
        }
      } catch (e) {
        failCount++
        if (failCount >= 5) {
          updateManualStep(stepIdx, {
            status: 'error',
            output: `${label} | Lost connection to server after ${failCount} retries`,
          })
          setManualRunId(null)
          setManualPipelineRunning(false)
          setManualActiveStep(null)
          setManualRemainingSpecs([])
          return true
        }
        updateManualStep(stepIdx, {
          output: `${label} | Reconnecting... (attempt ${failCount}/5)`,
        })
      }
      return false
    }

    poll().then((done) => { if (done) return })
    const interval = setInterval(async () => {
      const done = await poll()
      if (done) clearInterval(interval)
    }, 10000)

    // Resume polling immediately when tab becomes visible again
    const onVisible = () => {
      if (document.visibilityState === 'visible') poll()
    }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      clearInterval(interval)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [manualRunId, manualActiveStep])

  // Auto-continue: when a manual pipeline step completes, start next spec
  useEffect(() => {
    if (manualRunId !== null || !manualPipelineRunning || manualRemainingSpecs.length === 0) return
    if (manualCancelledRef.current) {
      setManualPipelineRunning(false)
      setManualRemainingSpecs([])
      return
    }
    const [next, ...rest] = manualRemainingSpecs
    setManualRemainingSpecs(rest)
    startManualPipelineStep(next)
  }, [manualRunId, manualPipelineRunning, manualRemainingSpecs])

  async function handleManualPipeline() {
    const steps = buildManualPipelineSteps()
    setManualPipelineSteps(steps)
    setManualPipelineRunning(true)
    setManualCosts([null, null, null])
    setManualRunId(null)
    setManualActiveStep(null)
    setManualRemainingSpecs([])
    manualCancelledRef.current = false

    // Start the 3-step pipeline: batch first, then incremental 2, then 3
    const [first, ...rest] = MANUAL_PIPELINE_SPECS
    setManualRemainingSpecs(rest)
    await startManualPipelineStep(first)
  }

  const [manualCancelling, setManualCancelling] = useState(false)

  async function handleCancelManualPipeline() {
    if (!manualRunId) return
    setManualCancelling(true)
    manualCancelledRef.current = true
    try {
      await apiCall(`/api/dataproc/cancel-run/${manualRunId}`, { method: 'POST' })
      addToast(`Cancellation requested for run ${manualRunId}`, 'info')
      if (manualActiveStep !== null) {
        updateManualStep(manualActiveStep, { output: `Cancellation requested for run ${manualRunId}. Waiting...` })
      }
    } catch (e: any) {
      addToast(`Failed to cancel: ${e.message}`, 'error')
    } finally {
      setManualCancelling(false)
    }
  }

  // ── Manual cluster teardown ─────────────────────────────────────────
  async function handleTeardownCluster(stepIdx: number) {
    const cfg = stepConfigs[stepIdx]
    setTeardownStatus(prev => prev.map((s, i) => i === stepIdx ? 'deleting' : s))
    try {
      const result = await apiCall('/api/dataproc/delete-cluster', {
        method: 'POST',
        body: JSON.stringify({
          project, region, cluster_name: cfg.clusterName,
        }),
      })
      if (result.status === 'deleted') {
        setTeardownStatus(prev => prev.map((s, i) => i === stepIdx ? 'deleted' : s))
        addToast(`Cluster ${cfg.clusterName} deleted`, 'success')
      } else if (result.status === 'not_found') {
        setTeardownStatus(prev => prev.map((s, i) => i === stepIdx ? 'not_found' : s))
        addToast(`Cluster ${cfg.clusterName} not found (already deleted?)`, 'info')
      }
    } catch (e: any) {
      setTeardownStatus(prev => prev.map((s, i) => i === stepIdx ? 'error' : s))
      addToast(`Teardown failed: ${e.message}`, 'error')
    }
  }

  // ── Manual prereq check ─────────────────────────────────────────────
  async function handleCheckManualPrereqs() {
    setManualPrereqChecking(true)
    setManualPrereqStatus('checking')

    // Build the initial items based on config
    const items: PrereqCheckItem[] = [
      { label: 'GCP Authentication', name: '', status: 'checking' },
      { label: 'VPC Network', name: 'tpcdi-benchmark-vpc', status: 'pending' },
      { label: 'Subnet', name: subnet, status: 'pending' },
      { label: 'Firewall (Internal)', name: 'tpcdi-benchmark-allow-internal', status: 'pending' },
      { label: 'Cloud NAT', name: 'tpcdi-benchmark-nat', status: 'pending' },
      { label: 'GCS Bucket', name: `gs://${gcsBucket}`, status: 'pending' },
    ]
    if (metastoreMode === 'create') {
      items.push({ label: 'Dataproc Metastore', name: metastoreName, status: 'pending' })
    }
    setManualPrereqs([...items])

    // Step 1: Check auth
    try {
      const auth = await apiCall('/api/dataproc/auth/status')
      if (auth.status !== 'authenticated') {
        items[0] = { ...items[0], status: 'error', name: '', error: auth.message || 'Not authenticated' }
        setManualPrereqs([...items])
        setManualPrereqStatus('error')
        setManualPrereqChecking(false)
        return
      }
      items[0] = { ...items[0], status: 'exists', name: auth.account || '' }
      setManualPrereqs([...items])
    } catch (e: any) {
      items[0] = { ...items[0], status: 'error', error: e.message }
      setManualPrereqs([...items])
      setManualPrereqStatus('error')
      setManualPrereqChecking(false)
      return
    }

    // Step 2: Mark infra items as checking
    for (let i = 1; i < items.length; i++) {
      items[i] = { ...items[i], status: 'checking' }
    }
    setManualPrereqs([...items])

    // Step 3: Call verify-prerequisites
    try {
      const prereqs = await apiCall('/api/dataproc/verify-prerequisites', {
        method: 'POST',
        body: JSON.stringify({
          project, region, subnet, gcs_bucket: gcsBucket,
          metastore_mode: metastoreMode,
          metastore_name: metastoreName,
          metastore_path: metastoreMode === 'create' ? `projects/${project}/locations/${region}/services/${metastoreName}` : '',
        }),
      })

      // Map backend steps to our items
      const stepMap: Record<string, number> = {
        vpc_network: 1,
        subnet: 2,
        firewall_internal: 3,
        cloud_nat: 4,
        gcs_bucket: 5,
        metastore: metastoreMode === 'create' ? 6 : -1,
      }

      for (const s of (prereqs.steps || [])) {
        const idx = stepMap[s.step]
        if (idx !== undefined && idx >= 0 && idx < items.length) {
          items[idx] = {
            ...items[idx],
            name: s.name || items[idx].name,
            status: s.status as PrereqCheckItem['status'],
            error: s.error,
            message: s.message,
          }
        }
      }

      setManualPrereqs([...items])

      if (prereqs.status === 'ready') {
        setManualPrereqStatus('ready')
      } else {
        setManualPrereqStatus('error')
      }
    } catch (e: any) {
      // Mark all pending/checking as error
      for (let i = 1; i < items.length; i++) {
        if (items[i].status === 'checking' || items[i].status === 'pending') {
          items[i] = { ...items[i], status: 'error', error: e.message }
        }
      }
      setManualPrereqs([...items])
      setManualPrereqStatus('error')
    } finally {
      setManualPrereqChecking(false)
    }
  }

  // ── Render ───────────────────────────────────────────────────────────

  return (
    <div>
      {toasts.map((t) => (
        <Toast key={t.id} message={t.message} type={t.type} onClose={() => removeToast(t.id)} />
      ))}

      {/* GCP Refresh Token (mandatory, auto-refreshes before every run) */}
      <div className={`mb-4 p-4 rounded-lg border ${
        gcpTokenUser ? 'bg-green-50 border-green-200'
          : gcpTokenError ? 'bg-red-50 border-red-200'
          : 'bg-slate-50 border-slate-200'
      }`}>
        <div className="mb-2">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm font-semibold text-slate-700">GCP Refresh Token</span>
            <span className="text-[10px] font-medium px-1.5 py-0.5 rounded bg-red-100 text-red-600">REQUIRED</span>
            {gcpTokenValidating && (
              <svg className="w-3.5 h-3.5 animate-spin text-blue-500" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            )}
          </div>
          <p className="text-[11px] text-slate-500">
            Auto-refreshes before every run. To get the token:
          </p>
          <ol className="text-[11px] text-slate-500 mt-1 ml-3 list-decimal space-y-0.5">
            <li>Run <code className="bg-slate-100 px-1 rounded text-[10px]">gcloud auth application-default login</code> (creates credentials file if it doesn't exist)</li>
            <li>Run <code className="bg-slate-100 px-1 rounded text-[10px]">cat ~/.config/gcloud/application_default_credentials.json</code></li>
            <li>Copy the <code className="bg-slate-100 px-1 rounded text-[10px]">refresh_token</code> field value and paste it below</li>
          </ol>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex-1 relative">
            <input
              type="password"
              value={gcpRefreshToken}
              onChange={(e) => setGcpRefreshToken(e.target.value)}
              placeholder="1//0xxx... (from application_default_credentials.json)"
              className={`w-full px-3 py-2 text-xs font-mono border rounded-md bg-white focus:ring-2 focus:outline-none ${
                gcpTokenUser ? 'border-green-300 focus:ring-green-300' :
                gcpTokenError ? 'border-red-300 focus:ring-red-300' :
                'border-slate-300 focus:ring-blue-400'
              }`}
            />
            {gcpTokenUser && (
              <div className="absolute right-2 top-1/2 -translate-y-1/2">
                <svg className="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
            )}
          </div>
          {gcpRefreshToken && (
            <button
              onClick={() => { setGcpRefreshToken(''); handleLogout() }}
              className="text-xs text-red-500 hover:text-red-700 underline flex-shrink-0"
            >
              Clear
            </button>
          )}
        </div>
        {gcpTokenUser && (
          <p className="mt-1.5 text-xs text-green-700">
            Authenticated as <span className="font-medium">{gcpTokenUser}</span> — token will auto-refresh before each run
          </p>
        )}
        {gcpTokenError && (
          <p className="mt-1.5 text-xs text-red-600">{gcpTokenError}</p>
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
            <ComputeModeSelector mode={computeMode} onChange={setComputeMode} platform="dataproc" />
            {computeMode === 'serverless' && (
              <div className="mb-4 ml-1">
                <label className="block text-xs font-medium text-slate-500 mb-1.5">Serverless Engine</label>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={() => setServerlessTier('standard')}
                    className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
                      serverlessTier === 'standard'
                        ? 'bg-blue-50 border-blue-300 text-blue-700'
                        : 'bg-white border-slate-200 text-slate-500 hover:border-slate-300'
                    }`}
                  >
                    Standard
                  </button>
                  <button
                    type="button"
                    onClick={() => setServerlessTier('premium')}
                    className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
                      serverlessTier === 'premium'
                        ? 'bg-amber-50 border-amber-300 text-amber-700'
                        : 'bg-white border-slate-200 text-slate-500 hover:border-slate-300'
                    }`}
                  >
                    Premium (Lightning)
                  </button>
                </div>
              </div>
            )}
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
              <FormField label="Target Database">
                <TextInput value={targetDatabase} onChange={setTargetDatabase} placeholder="tpcdi_warehouse" />
              </FormField>
              <FormField label="Dataproc Metastore">
                <TextInput value={metastoreName} onChange={setMetastoreName} placeholder="tpcdi-benchmark-metastore" />
                <p className="text-xs text-slate-500 mt-0.5">Creates if not exists, reuses if already created</p>
              </FormField>
              <ActionButton
                label={e2eRunning ? 'Running...' : !gcpTokenUser ? 'Provide GCP Token' : 'Run Full Pipeline'}
                onClick={handleRunE2E}
                loading={e2eRunning}
                disabled={e2eRunning || !gcpTokenUser}
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
      {/* Section 1: GCP & Network Configuration */}
      <Section title="1. GCP & Network Configuration" defaultOpen={true} colorScheme="emerald">
        <div className="pt-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
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
            <FormField label="Dataproc Metastore">
              <TextInput value={metastoreName} onChange={setMetastoreName} placeholder="tpcdi-benchmark-metastore" />
              <p className="text-xs text-slate-500 mt-0.5">Creates if not exists, reuses if already created</p>
            </FormField>
          </div>

          {/* Check Prerequisites */}
          <div className="mt-5 pt-4 border-t border-emerald-100">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h4 className="text-sm font-semibold text-slate-700">Infrastructure Prerequisites</h4>
                <p className="text-xs text-slate-500 mt-0.5">Verify and auto-create VPC, subnet, firewall, Cloud NAT, GCS bucket{metastoreMode === 'create' ? ', and Dataproc Metastore' : ''}</p>
              </div>
              <div className="flex items-center gap-3">
                {manualPrereqStatus === 'ready' && (
                  <span className="flex items-center gap-1.5 text-xs font-medium text-green-700 bg-green-50 px-2.5 py-1 rounded-full border border-green-200">
                    <span className="w-2 h-2 rounded-full bg-green-500" />
                    All Ready
                  </span>
                )}
                {manualPrereqStatus === 'error' && (
                  <span className="flex items-center gap-1.5 text-xs font-medium text-red-700 bg-red-50 px-2.5 py-1 rounded-full border border-red-200">
                    <span className="w-2 h-2 rounded-full bg-red-500" />
                    Issues Found
                  </span>
                )}
                <ActionButton
                  label={manualPrereqChecking ? 'Checking...' : manualPrereqStatus === 'ready' ? 'Re-check' : 'Check Prerequisites'}
                  onClick={handleCheckManualPrereqs}
                  loading={manualPrereqChecking}
                  disabled={manualPrereqChecking}
                  variant="secondary"
                />
              </div>
            </div>

            {/* Prereq Status Cards */}
            {manualPrereqs.length > 0 && (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {manualPrereqs.map((item, idx) => (
                  <div
                    key={idx}
                    className={`p-3 rounded-lg border transition-all ${
                      item.status === 'pending' ? 'bg-slate-50 border-slate-200' :
                      item.status === 'checking' ? 'bg-blue-50 border-blue-200' :
                      item.status === 'exists' ? 'bg-green-50 border-green-200' :
                      item.status === 'created' ? 'bg-emerald-50 border-emerald-200' :
                      item.status === 'creating' ? 'bg-blue-50 border-blue-300' :
                      item.status === 'skipped' ? 'bg-slate-50 border-slate-200' :
                      item.status === 'warning' ? 'bg-amber-50 border-amber-200' :
                      'bg-red-50 border-red-200'
                    }`}
                  >
                    <div className="flex items-start gap-2.5">
                      {/* Status Icon */}
                      <span className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0 ${
                        item.status === 'pending' ? 'bg-slate-200 text-slate-500' :
                        item.status === 'checking' ? 'bg-blue-400 text-white animate-pulse' :
                        item.status === 'exists' ? 'bg-green-500 text-white' :
                        item.status === 'created' ? 'bg-emerald-500 text-white' :
                        item.status === 'creating' ? 'bg-blue-500 text-white animate-pulse' :
                        item.status === 'skipped' ? 'bg-slate-400 text-white' :
                        item.status === 'warning' ? 'bg-amber-500 text-white' :
                        'bg-red-500 text-white'
                      }`}>
                        {item.status === 'exists' || item.status === 'created' ? '\u2713' :
                         item.status === 'checking' || item.status === 'creating' ? '\u2026' :
                         item.status === 'error' ? '\u00d7' :
                         item.status === 'warning' ? '!' :
                         item.status === 'skipped' ? '\u2013' :
                         '\u2022'}
                      </span>
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center justify-between">
                          <p className="text-xs font-semibold text-slate-700">{item.label}</p>
                          <span className={`text-[9px] font-medium uppercase px-1.5 py-0.5 rounded ${
                            item.status === 'pending' ? 'bg-slate-100 text-slate-500' :
                            item.status === 'checking' ? 'bg-blue-100 text-blue-600' :
                            item.status === 'exists' ? 'bg-green-100 text-green-700' :
                            item.status === 'created' ? 'bg-emerald-100 text-emerald-700' :
                            item.status === 'creating' ? 'bg-blue-100 text-blue-700' :
                            item.status === 'skipped' ? 'bg-slate-100 text-slate-500' :
                            item.status === 'warning' ? 'bg-amber-100 text-amber-700' :
                            'bg-red-100 text-red-700'
                          }`}>
                            {item.status === 'exists' ? 'exists' :
                             item.status === 'created' ? 'created' :
                             item.status === 'creating' ? 'creating...' :
                             item.status === 'checking' ? 'checking...' :
                             item.status}
                          </span>
                        </div>
                        {item.name && (
                          <p className="text-[10px] text-slate-500 font-mono mt-0.5 truncate" title={item.name}>{item.name}</p>
                        )}
                        {item.message && (
                          <p className="text-[10px] text-blue-600 mt-1">{item.message}</p>
                        )}
                        {item.error && (
                          <p className="text-[10px] text-red-600 mt-1 break-words">{item.error}</p>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </Section>

      {/* Section 2: Run Pipeline */}
      <Section title="2. Run Pipeline" defaultOpen={true} colorScheme="blue">
        <div className="pt-4">

          {/* Infrastructure Prerequisites status bar */}
          {manualPrereqStatus !== 'idle' && (
            <div className={`mb-4 p-3 rounded-lg border flex items-center gap-2 ${
              manualPrereqStatus === 'ready' ? 'bg-green-50 border-green-200' :
              manualPrereqStatus === 'checking' ? 'bg-blue-50 border-blue-200' :
              'bg-red-50 border-red-200'
            }`}>
              <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${
                manualPrereqStatus === 'ready' ? 'bg-green-500' :
                manualPrereqStatus === 'checking' ? 'bg-blue-500 animate-pulse' :
                'bg-red-500'
              }`} />
              <span className={`text-xs font-medium ${
                manualPrereqStatus === 'ready' ? 'text-green-700' :
                manualPrereqStatus === 'checking' ? 'text-blue-700' :
                'text-red-700'
              }`}>
                {manualPrereqStatus === 'ready' ? 'Infrastructure Prerequisites Ready' :
                 manualPrereqStatus === 'checking' ? 'Checking Infrastructure Prerequisites...' :
                 'Infrastructure Prerequisites - Issues Found'}
              </span>
              {manualPrereqStatus === 'ready' && (
                <span className="text-[10px] text-green-500 ml-1">
                  ({manualPrereqs.filter(p => p.status === 'exists' || p.status === 'created').length}/{manualPrereqs.length} passed)
                </span>
              )}
            </div>
          )}

          {/* Compute mode selector for manual */}
          <ComputeModeSelector mode={computeMode} onChange={setComputeMode} platform="dataproc" />
          {computeMode === 'serverless' && (
            <div className="mb-4 ml-1">
              <label className="block text-xs font-medium text-slate-500 mb-1.5">Serverless Engine</label>
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => setServerlessTier('standard')}
                  className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
                    serverlessTier === 'standard'
                      ? 'bg-blue-50 border-blue-300 text-blue-700'
                      : 'bg-white border-slate-200 text-slate-500 hover:border-slate-300'
                  }`}
                >
                  Standard
                </button>
                <button
                  type="button"
                  onClick={() => setServerlessTier('premium')}
                  className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
                    serverlessTier === 'premium'
                      ? 'bg-amber-50 border-amber-300 text-amber-700'
                      : 'bg-white border-slate-200 text-slate-500 hover:border-slate-300'
                  }`}
                >
                  Premium (Lightning)
                </button>
              </div>
            </div>
          )}

          {/* Shared config row */}
          <div className="mb-5 p-4 bg-slate-50 rounded-lg border border-slate-200">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
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
          </div>

          {/* Pipeline controls */}
          <div className="flex flex-wrap items-center gap-4 mb-6">
            <div className="text-sm text-slate-600">
              <span className="font-semibold">3-Step Pipeline:</span> Batch Load &rarr; Incremental Batch 2 &rarr; Incremental Batch 3
            </div>
            <ActionButton
              label={manualPipelineRunning ? 'Running...' : !gcpTokenUser ? 'Provide GCP Token' : 'Run Pipeline'}
              onClick={handleManualPipeline}
              loading={manualPipelineRunning}
              disabled={manualPipelineRunning || !gcpTokenUser}
            />
            {manualPipelineRunning && manualRunId && manualRunId !== 'pending' && (
              <button
                onClick={handleCancelManualPipeline}
                disabled={manualCancelling}
                className="px-4 py-2 text-sm font-medium rounded-md border border-red-300 text-red-600 hover:bg-red-50 disabled:opacity-50 transition-all"
              >
                {manualCancelling ? 'Cancelling...' : 'Cancel Run'}
              </button>
            )}
          </div>

          {/* Step Cards with per-step cluster config */}
          <div className="space-y-3">
            {manualPipelineSteps.map((step, i) => {
              const cfg = stepConfigs[i]
              const isExpanded = expandedStepConfig === i
              const tdStatus = teardownStatus[i]
              return (
              <div
                key={i}
                className={`rounded-lg border transition-all ${
                  step.status === 'pending' ? 'bg-slate-50 border-slate-200' :
                  step.status === 'running' ? 'bg-blue-50 border-blue-300 shadow-sm' :
                  step.status === 'success' ? 'bg-green-50 border-green-300' :
                  'bg-red-50 border-red-300'
                }`}
              >
                <div className="p-4">
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
                      <div>
                        <span className="text-sm font-semibold text-slate-700">{step.name}</span>
                        <p className="text-[10px] text-slate-400 font-mono">
                          <span className="text-slate-500">{cfg.clusterName}</span>
                          {' | '}{cfg.workerType} x{cfg.numWorkers}, {cfg.ssdCount} SSDs, {cfg.masterType} master
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {step.duration_ms !== null && (
                        <span className="text-xs font-mono text-slate-500">{formatDuration(step.duration_ms)}</span>
                      )}
                      {manualCosts[i] && (
                        <span className="text-xs font-mono text-blue-600">{formatCost(manualCosts[i]!.total_cost_usd)}</span>
                      )}
                      {step.link && (
                        <a href={step.link} target="_blank" rel="noopener noreferrer"
                          className="text-xs text-blue-600 hover:text-blue-700 underline">
                          GCP Console
                        </a>
                      )}
                      <span className={`text-[10px] font-medium uppercase px-2 py-0.5 rounded ${
                        step.status === 'pending' ? 'bg-slate-200 text-slate-500' :
                        step.status === 'running' ? 'bg-blue-100 text-blue-700' :
                        step.status === 'success' ? 'bg-green-100 text-green-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {step.status}
                      </span>
                      {!manualPipelineRunning && (
                        <button
                          onClick={() => setExpandedStepConfig(isExpanded ? null : i)}
                          className="text-[10px] text-slate-400 hover:text-slate-600 underline"
                        >
                          {isExpanded ? 'Hide' : 'Config'}
                        </button>
                      )}
                      {/* Teardown button */}
                      {(step.status === 'success' || step.status === 'error') && !manualPipelineRunning && (
                        <button
                          onClick={() => handleTeardownCluster(i)}
                          disabled={tdStatus === 'deleting'}
                          className={`text-[10px] font-medium px-2 py-0.5 rounded border transition-all ${
                            tdStatus === 'deleting' ? 'bg-amber-50 border-amber-300 text-amber-600 animate-pulse' :
                            tdStatus === 'deleted' ? 'bg-green-50 border-green-300 text-green-600' :
                            tdStatus === 'not_found' ? 'bg-slate-50 border-slate-300 text-slate-500' :
                            tdStatus === 'error' ? 'bg-red-50 border-red-300 text-red-600 hover:bg-red-100' :
                            'bg-red-50 border-red-200 text-red-600 hover:bg-red-100 hover:border-red-300'
                          }`}
                        >
                          {tdStatus === 'deleting' ? 'Deleting...' :
                           tdStatus === 'deleted' ? 'Deleted' :
                           tdStatus === 'not_found' ? 'Not Found' :
                           tdStatus === 'error' ? 'Retry Teardown' :
                           'Teardown Cluster'}
                        </button>
                      )}
                    </div>
                  </div>
                  {step.output && (
                    <p className="mt-2 text-xs text-slate-600 ml-10 break-words">{step.output}</p>
                  )}
                </div>

                {/* Expandable per-step cluster config */}
                {isExpanded && !manualPipelineRunning && (
                  <div className="px-4 pb-4 pt-0">
                    <div className="ml-10 p-3 bg-white rounded-lg border border-slate-200">
                      {computeMode === 'classic' ? (
                        <>
                          <p className="text-[10px] text-slate-500 uppercase font-semibold mb-2">Cluster Configuration</p>
                          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                            <FormField label="Cluster Name">
                              <TextInput value={cfg.clusterName} onChange={(v) => updateStepConfig(i, { clusterName: v })} placeholder="cluster-name" />
                            </FormField>
                            <FormField label="Master Type">
                              <SelectInput value={cfg.masterType} onChange={(v) => updateStepConfig(i, { masterType: v })} options={gcpNodeTypes} />
                            </FormField>
                            <FormField label="Worker Type">
                              <SelectInput value={cfg.workerType} onChange={(v) => updateStepConfig(i, { workerType: v })} options={gcpNodeTypes} />
                            </FormField>
                            <FormField label="Workers">
                              <NumberInput value={cfg.numWorkers} onChange={(v) => updateStepConfig(i, { numWorkers: v })} min={1} max={100} />
                            </FormField>
                            <FormField label="SSDs per Node">
                              <NumberInput value={cfg.ssdCount} onChange={(v) => updateStepConfig(i, { ssdCount: v })} min={0} max={8} />
                            </FormField>
                          </div>
                        </>
                      ) : (
                        <div className="flex items-center gap-2 px-3 py-2.5 bg-purple-50 border border-purple-200 rounded-lg text-sm text-purple-700">
                          <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                          </svg>
                          Serverless Batch mode — Dataproc auto-manages compute resources. No cluster config needed.
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
              )
            })}
          </div>

          {/* Cost Breakdown - per step */}
          {manualCosts.some(c => c !== null) && (
            <div className="mt-6 space-y-4">
              {(() => {
                const totalCost = manualCosts.reduce((sum, c) => sum + (c?.total_cost_usd || 0), 0)
                const totalDuration = manualCosts.reduce((sum, c) => sum + (c?.duration_ms || 0), 0)
                return (
                  <div className="p-4 bg-gradient-to-r from-blue-50 to-cyan-50 rounded-lg border border-blue-200">
                    <div className="flex items-center justify-between">
                      <h4 className="text-sm font-bold text-blue-800">Pipeline Total</h4>
                      <div className="flex items-center gap-6">
                        <div className="text-right">
                          <p className="text-[10px] text-blue-600 uppercase">Total Cost</p>
                          <p className="text-xl font-bold text-blue-700">{formatCost(totalCost)}</p>
                        </div>
                        <div className="text-right">
                          <p className="text-[10px] text-blue-600 uppercase">Total Duration</p>
                          <p className="text-xl font-bold text-blue-700">{formatDuration(totalDuration)}</p>
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })()}
              {manualCosts.map((cost, ci) => {
                if (!cost) return null
                const labels = ['Batch Load', 'Incremental Load - Batch 2', 'Incremental Load - Batch 3']
                return (
                  <div key={ci} className="p-4 bg-gradient-to-r from-slate-50 to-blue-50 rounded-lg border border-slate-200">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-sm font-bold text-slate-700">{labels[ci]} (SF {scaleFactor})</h4>
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
      </>
      )}

      {/* Run History */}
      <RunHistorySection platform="dataproc" />
    </div>
  )
}
