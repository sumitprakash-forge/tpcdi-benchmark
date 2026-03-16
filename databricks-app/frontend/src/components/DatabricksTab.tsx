import { useState, useEffect, useCallback, useRef } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput, Checkbox } from './FormField'
import ComputeModeSelector, { type ComputeMode } from './ComputeModeSelector'
import Toast from './Toast'
import RunHistorySection from './RunHistorySection'

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
const E2E_STATE_KEY = 'tpcdi_e2e_state'
const MANUAL_STATE_KEY = 'tpcdi_manual_pipeline_state'

interface E2EPersistedState {
  steps: E2EStep[]
  sf: string
  runId: number | null
  activeStep: number | null
  running: boolean
  costs: (RunCost | null)[]
  remainingSpecs: E2ERunSpec[]  // specs still to run after the active one completes
}

interface PrereqStep {
  step: string
  name: string
  status: 'exists' | 'created' | 'missing' | 'error' | 'skipped' | 'info'
  error?: string
  note?: string
  sa_email?: string
  gcs_bucket?: string
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

interface PrereqDetail {
  name: string
  label: string
  exists: boolean
  action?: string // 'exists' | 'created' | 'error' | 'info'
  url?: string
  error?: string
  note?: string
}

interface E2EStep {
  name: string
  status: 'pending' | 'running' | 'success' | 'error'
  output: string
  duration_ms: number | null
  link?: string
  details?: PrereqDetail[]
}

interface RunCost {
  vm_cost_usd: number
  dbu_cost_usd: number
  dbu_list_price: number
  total_dbus: number
  ssd_cost_usd: number
  total_cost_usd: number
  duration_hours: number
  duration_ms: number
  exec_duration_ms: number
  setup_duration_ms: number
  run_page_url: string
  cluster: {
    driver_node_type: string
    worker_node_type: string
    num_workers: number
    local_ssd_count: number
  }
}

interface E2ERunSizing {
  loadType: string
  batchId: string
  driver: string
  worker: string
  workers: number
  ssds: number
  label: string
}

function getE2ESizing(sf: number, loadType: string): Omit<E2ERunSizing, 'loadType' | 'batchId' | 'label'> {
  if (loadType === 'batch') {
    if (sf <= 100) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    if (sf <= 1000) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 5, ssds: 2 }
    return { driver: 'n2d-standard-8', worker: 'n2d-standard-16', workers: 6, ssds: 4 }
  } else {
    if (sf <= 100) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    if (sf <= 1000) return { driver: 'n2d-standard-8', worker: 'n2d-standard-8', workers: 3, ssds: 2 }
    return { driver: 'n2d-standard-8', worker: 'n2d-standard-16', workers: 5, ssds: 2 }
  }
}

function buildE2ESteps(sf: string): E2EStep[] {
  return [
    { name: 'Check & Create Prerequisites', status: 'pending', output: '', duration_ms: null },
    {
      name: `Batch Load (SF ${sf})`,
      status: 'pending', output: '', duration_ms: null,
    },
    {
      name: `Incremental Load - Batch 2 (SF ${sf})`,
      status: 'pending', output: '', duration_ms: null,
    },
    {
      name: `Incremental Load - Batch 3 (SF ${sf})`,
      status: 'pending', output: '', duration_ms: null,
    },
  ]
}

interface E2ERunSpec {
  loadType: string
  batchId: string
  stepIndex: number
}

const E2E_RUN_SPECS: E2ERunSpec[] = [
  { loadType: 'batch', batchId: '', stepIndex: 1 },
  { loadType: 'incremental', batchId: '2', stepIndex: 2 },
  { loadType: 'incremental', batchId: '3', stepIndex: 3 },
]

interface ManualPipelineStep {
  name: string
  status: 'pending' | 'running' | 'success' | 'error'
  output: string
  duration_ms: number | null
  link?: string
}

interface ManualPipelineSpec {
  loadType: string
  batchId: string
  stepIndex: number
  jobName: string
}

interface ManualPipelinePersistedState {
  steps: ManualPipelineStep[]
  running: boolean
  activeStep: number | null
  runId: number | null
  costs: (RunCost | null)[]
  remainingSpecs: ManualPipelineSpec[]
  sf: string
}

function buildManualPipelineSteps(sf: string): ManualPipelineStep[] {
  return [
    { name: `Batch Load (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
    { name: `Incremental Load - Batch 2 (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
    { name: `Incremental Load - Batch 3 (SF ${sf})`, status: 'pending', output: '', duration_ms: null },
  ]
}

function buildManualPipelineSpecs(sf: string): ManualPipelineSpec[] {
  return [
    { loadType: 'batch', batchId: '', stepIndex: 0, jobName: `TPC-DI Benchmark - Batch SF${sf}` },
    { loadType: 'incremental', batchId: '2', stepIndex: 1, jobName: `TPC-DI Benchmark - Inc B2 SF${sf}` },
    { loadType: 'incremental', batchId: '3', stepIndex: 2, jobName: `TPC-DI Benchmark - Inc B3 SF${sf}` },
  ]
}

export default function DatabricksTab() {
  // Mode toggle
  const [mode, setMode] = useState<'e2e' | 'manual'>('e2e')

  // Prerequisites state
  const [catalogName, setCatalogName] = useState('gcp_tpcdi_sandbox')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [prereqCheck, setPrereqCheck] = useState<PrereqCheckResult | null>(null)
  const [prereqResult, setPrereqResult] = useState<PrereqResult | null>(null)
  const [loadingPrereqCheck, setLoadingPrereqCheck] = useState(false)
  const [loadingPrereqCreate, setLoadingPrereqCreate] = useState(false)
  const [fixingGcsIam, setFixingGcsIam] = useState(false)
  const [adminToken, setAdminToken] = useState(() => localStorage.getItem('tpcdi_databricks_pat') || '')
  const [tokenUser, setTokenUser] = useState<{ user_name: string; display_name: string } | null>(null)
  const [resolvingToken, setResolvingToken] = useState(false)
  const [tokenError, setTokenError] = useState<string | null>(null)

  // Benchmark config state
  const [loadType, _setLoadType] = useState('batch')
  const [scaleFactor, setScaleFactor] = useState('10')
  const [targetCatalog, setTargetCatalog] = useState('gcp_tpcdi_sandbox')
  const [targetSchema, setTargetSchema] = useState('dw')
  const [_batchId, _setBatchId] = useState('')
  const [computeMode, setComputeMode] = useState<ComputeMode>('classic')
  const [cloud, setCloud] = useState('GCP')
  const [xmlFormat, setXmlFormat] = useState('com.databricks.spark.xml')
  const [sparkVersion, setSparkVersion] = useState('17.3.x-scala2.13')
  const [workerNodeType, setWorkerNodeType] = useState('n2-highmem-16')
  const [driverNodeType, setDriverNodeType] = useState('n2-highmem-16')
  const [numWorkers, setNumWorkers] = useState(3)
  const [jarPath, setJarPath] = useState('')
  const [localSsds, setLocalSsds] = useState(2)
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
  const [jobId, _setJobId] = useState<number | null>(() => {
    const saved = localStorage.getItem(STORAGE_KEY)
    return saved ? parseInt(saved, 10) : null
  })
  const [runId, setRunId] = useState<number | null>(null)
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null)
  const [_jobRuns, setJobRuns] = useState<RunInfo[]>([])
  const [_loadingRuns, setLoadingRuns] = useState(false)
  const [_availableJobs, setAvailableJobs] = useState<JobInfo[]>([])
  const [_loadingJobs, setLoadingJobs] = useState(false)
  const [_manualJobId, _setManualJobId] = useState('')

  // End-to-End state - restore from localStorage
  const savedE2E = useRef<E2EPersistedState | null>((() => {
    try {
      const raw = localStorage.getItem(E2E_STATE_KEY)
      return raw ? JSON.parse(raw) : null
    } catch { return null }
  })())
  const [e2eSteps, setE2eSteps] = useState<E2EStep[]>(savedE2E.current?.steps || buildE2ESteps('1000'))
  const [e2eRunning, setE2eRunning] = useState(savedE2E.current?.running || false)
  const [e2eSF, setE2eSF] = useState(savedE2E.current?.sf || '1000')
  const [e2eRunId, setE2eRunId] = useState<number | null>(savedE2E.current?.runId || null)
  const [e2eActiveStep, setE2eActiveStep] = useState<number | null>(savedE2E.current?.activeStep || null)
  const [runCosts, setRunCosts] = useState<(RunCost | null)[]>(savedE2E.current?.costs || [null, null, null])
  const [e2eRemainingSpecs, setE2eRemainingSpecs] = useState<E2ERunSpec[]>(savedE2E.current?.remainingSpecs || [])
  const [e2eSaveNeeded, setE2eSaveNeeded] = useState(false)
  const benchmarkNotebookPath = useRef<string>('')

  // Manual pipeline state - restore from localStorage
  const savedManual = useRef<ManualPipelinePersistedState | null>((() => {
    try {
      const raw = localStorage.getItem(MANUAL_STATE_KEY)
      return raw ? JSON.parse(raw) : null
    } catch { return null }
  })())
  const [manualSteps, setManualSteps] = useState<ManualPipelineStep[]>(savedManual.current?.steps || buildManualPipelineSteps('10'))
  const [manualRunning, setManualRunning] = useState(savedManual.current?.running || false)
  const [manualActiveStep, setManualActiveStep] = useState<number | null>(savedManual.current?.activeStep ?? null)
  const [manualRunId, setManualRunId] = useState<number | null>(savedManual.current?.runId ?? null)
  const [manualCosts, setManualCosts] = useState<(RunCost | null)[]>(savedManual.current?.costs || [null, null, null])
  const [manualRemainingSpecs, setManualRemainingSpecs] = useState<ManualPipelineSpec[]>(savedManual.current?.remainingSpecs || [])
  const manualCancelledRef = useRef(false)
  const manualStartingRef = useRef(false)
  const [manualSaveNeeded, setManualSaveNeeded] = useState(false)

  // Persist PAT token to localStorage
  useEffect(() => {
    if (adminToken) {
      localStorage.setItem('tpcdi_databricks_pat', adminToken)
    } else {
      localStorage.removeItem('tpcdi_databricks_pat')
    }
  }, [adminToken])

  // Persist jobId to localStorage
  useEffect(() => {
    if (jobId) {
      localStorage.setItem(STORAGE_KEY, String(jobId))
    } else {
      localStorage.removeItem(STORAGE_KEY)
    }
  }, [jobId])

  // Persist E2E state to localStorage — never persist running=true to avoid stale spinners on refresh
  useEffect(() => {
    const state: E2EPersistedState = {
      steps: e2eSteps,
      sf: e2eSF,
      runId: e2eRunId,
      activeStep: e2eActiveStep,
      running: false,  // Always persist as false — polling useEffect resumes if runId is active
      costs: runCosts,
      remainingSpecs: e2eRemainingSpecs,
    }
    localStorage.setItem(E2E_STATE_KEY, JSON.stringify(state))
  }, [e2eSteps, e2eSF, e2eRunId, e2eActiveStep, e2eRunning, runCosts, e2eRemainingSpecs])

  // Auto-save E2E run history when pipeline completes (uses fresh state, no stale closure)
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
          platform: 'databricks',
          mode: 'e2e',
          flow_type: 'e2e',
          compute_mode: computeMode,
          scale_factor: e2eSF,
          steps: e2eSteps,
          costs: runCosts,
          completed_at: new Date().toISOString(),
          status: e2eSteps.every(s => s.status === 'success') ? 'completed' : 'failed',
          run_by: tokenUser?.display_name || tokenUser?.user_name || 'unknown',
          run_by_email: tokenUser?.user_name || '',
        }
      })
    }).catch(() => {})
  }, [e2eSaveNeeded, e2eSteps, e2eSF, runCosts, tokenUser])

  // Auto-save manual pipeline run history when complete (uses fresh state)
  useEffect(() => {
    if (!manualSaveNeeded) return
    setManualSaveNeeded(false)
    const hasSuccess = manualSteps.some(s => s.status === 'success')
    if (!hasSuccess) return
    fetch('/api/databricks/save-run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        run_data: {
          platform: 'databricks',
          mode: 'manual',
          flow_type: 'manual',
          compute_mode: computeMode,
          scale_factor: scaleFactor,
          steps: manualSteps,
          costs: manualCosts,
          completed_at: new Date().toISOString(),
          status: manualSteps.every(s => s.status === 'success') ? 'completed' : 'failed',
          run_by: tokenUser?.display_name || tokenUser?.user_name || 'unknown',
          run_by_email: tokenUser?.user_name || '',
        }
      })
    }).catch(() => {})
  }, [manualSaveNeeded, manualSteps, scaleFactor, manualCosts, tokenUser])

  // Persist manual pipeline state to localStorage
  useEffect(() => {
    const state: ManualPipelinePersistedState = {
      steps: manualSteps,
      running: manualRunning,
      activeStep: manualActiveStep,
      runId: manualRunId,
      costs: manualCosts,
      remainingSpecs: manualRemainingSpecs,
      sf: scaleFactor,
    }
    localStorage.setItem(MANUAL_STATE_KEY, JSON.stringify(state))
  }, [manualSteps, manualRunning, manualActiveStep, manualRunId, manualCosts, manualRemainingSpecs, scaleFactor])

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

  // Resolve PAT token to user info
  const resolveToken = useCallback(async (token: string) => {
    if (!token || token.length < 10) {
      setTokenUser(null)
      setTokenError(null)
      return
    }
    setResolvingToken(true)
    setTokenError(null)
    try {
      const data = await apiCall('/api/databricks/resolve-token', {
        method: 'POST',
        body: JSON.stringify({ token }),
      })
      setTokenUser({ user_name: data.user_name, display_name: data.display_name })
    } catch (e: any) {
      setTokenUser(null)
      setTokenError(e.message || 'Invalid token')
    } finally {
      setResolvingToken(false)
    }
  }, [])

  // Debounced token resolution
  useEffect(() => {
    if (!adminToken) { setTokenUser(null); setTokenError(null); return }
    const timer = setTimeout(() => resolveToken(adminToken), 500)
    return () => clearTimeout(timer)
  }, [adminToken, resolveToken])

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
          admin_token: adminToken,
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

  async function handleFixGcsIam(saEmail: string, gcsBucket: string) {
    setFixingGcsIam(true)
    try {
      const data = await apiCall('/api/databricks/grant-gcs-iam', {
        method: 'POST',
        body: JSON.stringify({ sa_email: saEmail, gcs_bucket: gcsBucket }),
      })
      addToast(data.message || 'GCS IAM grant applied', 'success')
      // Re-run prerequisites to pick up the fix
      await handleCreatePrerequisites()
    } catch (e: any) {
      addToast(`GCS IAM fix failed: ${e.message}. Sign in on Dataproc tab first.`, 'error')
    } finally {
      setFixingGcsIam(false)
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


  // ── End-to-End Run ─────────────────────────────────────────────────────

  function updateE2EStep(index: number, updates: Partial<E2EStep>) {
    setE2eSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s))
  }

  const e2eCancelledRef = useRef(false)
  const e2eStartingRef = useRef(false)  // gates auto-continue while a spec is being kicked off
  const [e2eNextSpec, setE2eNextSpec] = useState<E2ERunSpec | null>(null)

  // Build run label from spec
  function buildRunLabel(spec: E2ERunSpec, sf: number): string {
    const sizing = getE2ESizing(sf, spec.loadType)
    return spec.loadType === 'batch'
      ? `SF ${sf} | Batch | ${sizing.worker} x${sizing.workers}, ${sizing.ssds} SSDs`
      : `SF ${sf} | Incremental Batch ${spec.batchId} | ${sizing.worker} x${sizing.workers}, ${sizing.ssds} SSDs`
  }

  // Kick off the next run spec: create job → start run → set e2eRunId (polling picks it up)
  async function startE2ERunSpec(spec: E2ERunSpec) {
    e2eStartingRef.current = true  // prevent auto-continue from firing during async setup
    const sf = parseInt(e2eSF)
    const { loadType, batchId, stepIndex } = spec
    const sizing = getE2ESizing(sf, loadType)
    const runLabel = buildRunLabel(spec, sf)

    setE2eActiveStep(stepIndex)
    updateE2EStep(stepIndex, { status: 'running', output: `${runLabel} | Creating job...` })

    let createdJobId: number | null = null
    try {
      const jobResult = await apiCall('/api/databricks/create-job', {
        method: 'POST',
        body: JSON.stringify({
          job_name: `TPC-DI Benchmark SF${sf} ${loadType}${batchId ? ' batch' + batchId : ''}`,
          load_type: loadType,
          scale_factor: sf,
          raw_data_path: rawDataPath,
          catalog: catalogName,
          schema_name: targetSchema,
          batch_id: batchId,
          cloud: 'GCP',
          xml_format: 'com.databricks.spark.xml',
          spark_version: '17.3.x-scala2.13',
          worker_node_type: sizing.worker,
          driver_node_type: sizing.driver,
          num_workers: sizing.workers,
          jar_path: `/Volumes/${catalogName}/${targetSchema}/libs/spark-xml_2.13-0.18.0.jar`,
          notebook_path: benchmarkNotebookPath.current || '',
          photon: true,
          local_ssds: sizing.ssds,
          log_detailed_stats: true,
          admin_token: adminToken,
        }),
      })
      createdJobId = jobResult.job_id
      updateE2EStep(stepIndex, { output: `${runLabel} | Job ${jobResult.action} (ID: ${jobResult.job_id}). Starting run...` })
    } catch (e: any) {
      updateE2EStep(stepIndex, { status: 'error', output: `${runLabel} | Job creation failed: ${e.message}` })
      setE2eRunning(false)
      setE2eRemainingSpecs([])
      e2eStartingRef.current = false
      return
    }

    try {
      const runResult = await apiCall('/api/databricks/run-job', {
        method: 'POST',
        body: JSON.stringify({ job_id: createdJobId, admin_token: adminToken }),
      })
      setE2eRunId(runResult.run_id)
      updateE2EStep(stepIndex, {
        status: 'running',
        output: `${runLabel} | Run ${runResult.run_id} started. Polling...`,
        link: runResult.run_page_url || undefined,
      })
    } catch (e: any) {
      updateE2EStep(stepIndex, { status: 'error', output: `${runLabel} | ${e.message}` })
      setE2eRunning(false)
      setE2eRemainingSpecs([])
    }
    e2eStartingRef.current = false
  }

  // Polling useEffect - runs whenever e2eRunId changes (including after page refresh)
  useEffect(() => {
    if (!e2eRunId || e2eActiveStep === null) return
    // If we have a valid runId, mark as running (handles page refresh resume)
    setE2eRunning(true)
    const stepIdx = e2eActiveStep
    const sf = parseInt(e2eSF)
    // Find matching spec to build label
    const spec = E2E_RUN_SPECS.find(s => s.stepIndex === stepIdx)
    const runLabel = spec ? buildRunLabel(spec, sf) : `Step ${stepIdx}`

    const poll = async () => {
      try {
        const tokenParam = adminToken ? `&admin_token=${encodeURIComponent(adminToken)}` : ''
        const status = await apiCall(`/api/databricks/job-status/${e2eRunId}?_=1${tokenParam}`)
        const stateStr = status.result_state
          ? `${status.state} (${status.result_state})`
          : status.state

        updateE2EStep(stepIdx, {
          output: `${runLabel} | Status: ${stateStr}. Setup: ${formatDuration(status.setup_duration)}, Exec: ${formatDuration(status.execution_duration)}`,
          link: status.run_page_url || undefined,
          duration_ms: (status.execution_duration || 0) + (status.setup_duration || 0),
        })

        const terminal = status.state === 'TERMINATED' || status.state === 'SKIPPED' || status.state === 'INTERNAL_ERROR'
        if (terminal) {
          const isSuccess = status.state === 'TERMINATED' && status.result_state === 'SUCCESS'
          updateE2EStep(stepIdx, {
            status: isSuccess ? 'success' : 'error',
            output: `${runLabel} | ${stateStr}. Setup: ${formatDuration(status.setup_duration)}, Exec: ${formatDuration(status.execution_duration)}${status.state_message ? ' - ' + status.state_message : ''}`,
            link: status.run_page_url || undefined,
            duration_ms: (status.execution_duration || 0) + (status.setup_duration || 0),
          })
          // Fetch cost
          if (isSuccess) {
            try {
              const costTokenParam = adminToken ? `?admin_token=${encodeURIComponent(adminToken)}` : ''
              const costData = await apiCall(`/api/databricks/run-cost/${e2eRunId}${costTokenParam}`)
              setRunCosts(prev => {
                const next = [...prev]
                next[stepIdx - 1] = costData
                return next
              })
            } catch { /* ignore */ }
          }
          setE2eRunId(null)
          // If failed or cancelled, stop pipeline
          if (!isSuccess) {
            setE2eRunning(false)
            setE2eActiveStep(null)
            setE2eRemainingSpecs([])
          } else {
            // Signal auto-continue by popping next spec
            setE2eRemainingSpecs(prev => {
              if (e2eCancelledRef.current) {
                setE2eRunning(false)
                return []
              }
              if (prev.length === 0) {
                // Pipeline complete — save via useEffect (avoids stale closure)
                setE2eRunning(false)
                setE2eActiveStep(null)
                setE2eSaveNeeded(true)
                return prev
              }
              const [next, ...rest] = prev
              setE2eNextSpec(next)
              return rest
            })
          }
          return true
        }
      } catch { /* ignore */ }
      return false
    }

    // Initial poll
    poll().then((done) => {
      if (done) return
    })
    const interval = setInterval(async () => {
      const done = await poll()
      if (done) clearInterval(interval)
    }, 10000)
    return () => clearInterval(interval)
  }, [e2eRunId, e2eActiveStep, adminToken])

  // Auto-continue: when e2eNextSpec is set, start the next run
  useEffect(() => {
    if (!e2eNextSpec) return
    const spec = e2eNextSpec
    setE2eNextSpec(null)
    startE2ERunSpec(spec)
  }, [e2eNextSpec])

  // Main E2E handler: prereqs then queue all run specs
  async function handleRunE2E() {
    if (!adminToken || !tokenUser) {
      addToast('PAT token is required. Enter a valid Databricks PAT token to run jobs.', 'error')
      return
    }
    const steps = buildE2ESteps(e2eSF)
    setE2eSteps(steps)
    setE2eRunning(true)
    setRunCosts([null, null, null])
    setE2eRunId(null)
    setE2eActiveStep(null)
    setE2eRemainingSpecs([])
    e2eCancelledRef.current = false

    // Step 0: Prerequisites
    updateE2EStep(0, { status: 'running', output: 'Checking prerequisites...', details: [] })
    const step0Start = Date.now()
    try {
      const checkData = await apiCall(`/api/databricks/check-prerequisites?catalog_name=${encodeURIComponent(catalogName)}&schema_name=${encodeURIComponent(targetSchema)}&raw_data_path=${encodeURIComponent(rawDataPath)}`)
      const checkDetails: PrereqDetail[] = Object.entries(checkData.resources || {}).map(([key, res]: [string, any]) => ({
        name: res.name,
        label: key.replace(/_/g, ' '),
        exists: res.exists,
        action: res.exists ? 'exists' : 'missing',
        url: res.url || undefined,
      }))
      if (checkData.status === 'ready') {
        updateE2EStep(0, { status: 'success', output: 'All prerequisites already exist.', duration_ms: Date.now() - step0Start, details: checkDetails })
      } else {
        updateE2EStep(0, { output: 'Some prerequisites missing. Creating...', details: checkDetails })
        const createData = await apiCall('/api/databricks/verify-prerequisites', {
          method: 'POST',
          body: JSON.stringify({ catalog_name: catalogName, schema_name: targetSchema, raw_data_path: rawDataPath, admin_token: adminToken }),
        })
        const createDetails: PrereqDetail[] = (createData.steps || []).map((s: any) => ({
          name: s.name,
          label: s.step.replace(/_/g, ' '),
          exists: s.status === 'exists' || s.status === 'created' || s.status === 'info',
          action: s.status,
          url: s.url || undefined,
          error: s.error || undefined,
          note: s.note || undefined,
        }))
        if (createData.status === 'ready') {
          updateE2EStep(0, { status: 'success', output: 'All prerequisites ready.', duration_ms: Date.now() - step0Start, details: createDetails })
        } else {
          const errors = (createData.steps || []).filter((s: any) => s.status === 'error').map((s: any) => `${s.step}: ${s.error}`).join('; ')
          updateE2EStep(0, { status: 'error', output: `Failed: ${errors}`, duration_ms: Date.now() - step0Start, details: createDetails })
          setE2eRunning(false)
          return
        }
      }
      await checkPrerequisites()

      // Bootstrap: deploy notebooks + JAR to workspace
      updateE2EStep(0, { output: 'Deploying notebooks & spark-xml JAR...' })
      const bootstrap = await apiCall('/api/databricks/bootstrap', {
        method: 'POST',
        body: JSON.stringify({ catalog_name: catalogName, schema_name: targetSchema }),
      })
      const bootstrapDetails: PrereqDetail[] = (bootstrap.steps || []).map((s: any) => ({
        name: s.name,
        label: s.step.replace(/_/g, ' '),
        exists: s.status === 'exists' || s.status === 'created',
        action: s.status,
        error: s.error || undefined,
      }))
      if (bootstrap.status !== 'ready') {
        const errors = (bootstrap.steps || []).filter((s: any) => s.status === 'error').map((s: any) => `${s.step}: ${s.error}`).join('; ')
        updateE2EStep(0, { status: 'error', output: `Bootstrap failed: ${errors}`, duration_ms: Date.now() - step0Start, details: bootstrapDetails })
        setE2eRunning(false)
        return
      }
      benchmarkNotebookPath.current = bootstrap.benchmark_notebook || ''
      updateE2EStep(0, { status: 'success', output: 'Prerequisites ready. Notebooks & JAR deployed.', duration_ms: Date.now() - step0Start })
    } catch (e: any) {
      updateE2EStep(0, { status: 'error', output: e.message, duration_ms: Date.now() - step0Start })
      setE2eRunning(false)
      return
    }

    // Queue all run specs - first one starts immediately, rest are remaining
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
      await apiCall(`/api/databricks/cancel-run/${e2eRunId}?admin_token=${encodeURIComponent(adminToken)}`, { method: 'POST' })
      addToast(`Run ${e2eRunId} cancellation requested`, 'info')
      if (e2eActiveStep !== null) {
        updateE2EStep(e2eActiveStep, { output: `Cancellation requested for run ${e2eRunId}. Waiting for confirmation...` })
      }
    } catch (e: any) {
      addToast(`Failed to cancel: ${e.message}`, 'error')
    } finally {
      setCancelling(false)
    }
  }

  // ── Manual Pipeline ────────────────────────────────────────────────────

  function updateManualStep(index: number, updates: Partial<ManualPipelineStep>) {
    setManualSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s))
  }

  async function startManualRunSpec(spec: ManualPipelineSpec) {
    manualStartingRef.current = true
    const { loadType: lt, batchId: bid, stepIndex, jobName } = spec

    setManualActiveStep(stepIndex)
    updateManualStep(stepIndex, { status: 'running', output: `Creating job "${jobName}"...` })

    let createdJobId: number | null = null
    try {
      const jobResult = await apiCall('/api/databricks/create-job', {
        method: 'POST',
        body: JSON.stringify({
          job_name: jobName,
          load_type: lt,
          scale_factor: parseInt(scaleFactor),
          raw_data_path: rawDataPath,
          catalog: targetCatalog,
          schema_name: targetSchema,
          batch_id: bid,
          cloud,
          xml_format: xmlFormat,
          spark_version: sparkVersion,
          worker_node_type: workerNodeType,
          driver_node_type: driverNodeType,
          num_workers: numWorkers,
          jar_path: effectiveJarPath,
          photon,
          local_ssds: localSsds,
          log_detailed_stats: logDetailedStats,
          admin_token: adminToken,
        }),
      })
      createdJobId = jobResult.job_id
      updateManualStep(stepIndex, { output: `Job ${jobResult.action} (ID: ${jobResult.job_id}). Starting run...` })
    } catch (e: any) {
      updateManualStep(stepIndex, { status: 'error', output: `Job creation failed: ${e.message}` })
      setManualRunning(false)
      setManualRemainingSpecs([])
      manualStartingRef.current = false
      return
    }

    try {
      const runResult = await apiCall('/api/databricks/run-job', {
        method: 'POST',
        body: JSON.stringify({ job_id: createdJobId, admin_token: adminToken }),
      })
      setManualRunId(runResult.run_id)
      updateManualStep(stepIndex, {
        status: 'running',
        output: `Run ${runResult.run_id} started. Polling...`,
        link: runResult.run_page_url || undefined,
      })
    } catch (e: any) {
      updateManualStep(stepIndex, { status: 'error', output: `Run start failed: ${e.message}` })
      setManualRunning(false)
      setManualRemainingSpecs([])
    }
    manualStartingRef.current = false
  }

  // Manual pipeline polling
  useEffect(() => {
    if (!manualRunId || manualActiveStep === null) return
    const stepIdx = manualActiveStep

    const poll = async () => {
      try {
        const mTokenParam = adminToken ? `&admin_token=${encodeURIComponent(adminToken)}` : ''
        const status = await apiCall(`/api/databricks/job-status/${manualRunId}?_=1${mTokenParam}`)
        const stateStr = status.result_state
          ? `${status.state} (${status.result_state})`
          : status.state

        updateManualStep(stepIdx, {
          output: `Status: ${stateStr}. Setup: ${formatDuration(status.setup_duration)}, Exec: ${formatDuration(status.execution_duration)}`,
          link: status.run_page_url || undefined,
          duration_ms: (status.execution_duration || 0) + (status.setup_duration || 0),
        })

        const terminal = status.state === 'TERMINATED' || status.state === 'SKIPPED' || status.state === 'INTERNAL_ERROR'
        if (terminal) {
          const isSuccess = status.state === 'TERMINATED' && status.result_state === 'SUCCESS'
          updateManualStep(stepIdx, {
            status: isSuccess ? 'success' : 'error',
            output: `${stateStr}. Setup: ${formatDuration(status.setup_duration)}, Exec: ${formatDuration(status.execution_duration)}${status.state_message ? ' - ' + status.state_message : ''}`,
            link: status.run_page_url || undefined,
            duration_ms: (status.execution_duration || 0) + (status.setup_duration || 0),
          })
          if (isSuccess) {
            try {
              const mCostParam = adminToken ? `?admin_token=${encodeURIComponent(adminToken)}` : ''
              const costData = await apiCall(`/api/databricks/run-cost/${manualRunId}${mCostParam}`)
              setManualCosts(prev => {
                const next = [...prev]
                next[stepIdx] = costData
                return next
              })
            } catch { /* ignore */ }
          }
          setManualRunId(null)
          if (!isSuccess) {
            setManualRunning(false)
            setManualActiveStep(null)
            setManualRemainingSpecs([])
          }
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
  }, [manualRunId, manualActiveStep, adminToken])

  // Manual pipeline auto-continue
  useEffect(() => {
    if (manualRunId !== null || !manualRunning) return
    if (manualCancelledRef.current) {
      setManualRunning(false)
      setManualRemainingSpecs([])
      return
    }
    if (manualRemainingSpecs.length === 0) {
      setManualRunning(false)
      setManualActiveStep(null)
      setManualSaveNeeded(true)
      return
    }
    if (manualStartingRef.current) return
    const [next, ...rest] = manualRemainingSpecs
    setManualRemainingSpecs(rest)
    startManualRunSpec(next)
  }, [manualRunId, manualRunning, manualRemainingSpecs])

  async function handleManualPipeline() {
    if (!adminToken || !tokenUser) {
      addToast('PAT token is required. Enter a valid Databricks PAT token to run jobs.', 'error')
      return
    }
    const steps = buildManualPipelineSteps(scaleFactor)
    setManualSteps(steps)
    setManualRunning(true)
    setManualCosts([null, null, null])
    setManualRunId(null)
    setManualActiveStep(null)
    setManualRemainingSpecs([])
    manualCancelledRef.current = false

    const specs = buildManualPipelineSpecs(scaleFactor)
    const [first, ...rest] = specs
    setManualRemainingSpecs(rest)
    await startManualRunSpec(first)
  }

  const [manualCancelling, setManualCancelling] = useState(false)

  async function handleCancelManualPipeline() {
    if (!manualRunId) return
    setManualCancelling(true)
    manualCancelledRef.current = true
    try {
      await apiCall(`/api/databricks/cancel-run/${manualRunId}?admin_token=${encodeURIComponent(adminToken)}`, { method: 'POST' })
      addToast(`Run ${manualRunId} cancellation requested`, 'info')
      if (manualActiveStep !== null) {
        updateManualStep(manualActiveStep, { output: `Cancellation requested for run ${manualRunId}. Waiting for confirmation...` })
      }
    } catch (e: any) {
      addToast(`Failed to cancel: ${e.message}`, 'error')
    } finally {
      setManualCancelling(false)
    }
  }

  function formatCost(usd: number) {
    return `$${usd.toFixed(4)}`
  }

  // ── Status Polling ─────────────────────────────────────────────────────

  const pollStatus = useCallback(async () => {
    if (!runId) return
    try {
      const pollTokenParam = adminToken ? `?admin_token=${encodeURIComponent(adminToken)}` : ''
      const status = await apiCall(`/api/databricks/job-status/${runId}${pollTokenParam}`)
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

  return (
    <div>
      {toasts.map((t) => (
        <Toast key={t.id} message={t.message} type={t.type} onClose={() => removeToast(t.id)} />
      ))}

      {/* Mode Toggle */}
      <div className="flex items-center gap-1 mb-6 p-1 bg-slate-100 rounded-lg w-fit">
        <button
          onClick={() => setMode('e2e')}
          className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
            mode === 'e2e'
              ? 'bg-white text-orange-700 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          End to End
        </button>
        <button
          onClick={() => setMode('manual')}
          className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
            mode === 'manual'
              ? 'bg-white text-orange-700 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          Manual
        </button>
      </div>

      {/* Compute Mode Selector — applies to both E2E and Manual */}
      <div className="mb-6">
        <ComputeModeSelector mode={computeMode} onChange={setComputeMode} platform="databricks" />
      </div>

      {/* ── End-to-End Mode ─────────────────────────────────────────── */}
      {mode === 'e2e' && (
        <Section title="End-to-End Benchmark Run" defaultOpen={true} colorScheme="blue">
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
              <FormField label="Catalog">
                <input className="px-3 py-2 bg-white border border-slate-200 rounded-md text-sm font-mono text-slate-700" value={catalogName} onChange={e => setCatalogName(e.target.value)} />
              </FormField>
              <FormField label="Raw Data Path">
                <div className="px-3 py-2 bg-slate-50 border border-slate-200 rounded-md text-sm font-mono text-slate-700 max-w-[260px] truncate" title={rawDataPath}>{rawDataPath}</div>
              </FormField>
              <FormField label="Databricks PAT Token (required)">
                <input
                  type="password"
                  className={`px-3 py-2 bg-white border rounded-md text-sm font-mono text-slate-700 ${tokenError ? 'border-red-300' : tokenUser ? 'border-green-300' : 'border-slate-200'}`}
                  value={adminToken}
                  onChange={e => setAdminToken(e.target.value)}
                  placeholder="dapi..."
                  title="Databricks PAT token — jobs run as this user"
                />
                {resolvingToken && (
                  <p className="text-xs text-blue-500 mt-1">Resolving token...</p>
                )}
                {tokenUser && (
                  <p className="text-xs text-green-600 mt-1 flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-green-400 inline-block" />
                    Running as: <span className="font-medium">{tokenUser.display_name || tokenUser.user_name}</span>
                    {tokenUser.display_name && tokenUser.user_name && tokenUser.display_name !== tokenUser.user_name && (
                      <span className="text-slate-400">({tokenUser.user_name})</span>
                    )}
                  </p>
                )}
                {tokenError && (
                  <p className="text-xs text-red-500 mt-1">{tokenError}</p>
                )}
                {!adminToken && (
                  <p className="text-xs text-slate-400 mt-1">Jobs will run as this user. All permissions use this identity.</p>
                )}
              </FormField>
              <ActionButton
                label={e2eRunning ? 'Running...' : 'Run Full Pipeline'}
                onClick={handleRunE2E}
                loading={e2eRunning}
                disabled={e2eRunning || !adminToken || !tokenUser}
              />
              {e2eRunning && e2eRunId && (
                <button
                  onClick={handleCancelE2E}
                  disabled={cancelling}
                  className="px-4 py-2 text-sm font-medium rounded-md border border-red-300 text-red-600 hover:bg-red-50 disabled:opacity-50 transition-all"
                >
                  {cancelling ? 'Cancelling...' : 'Cancel Run'}
                </button>
              )}
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
                      {step.link && (
                        <a
                          href={step.link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs text-orange-600 hover:text-orange-700 underline"
                        >
                          View Run
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
                    </div>
                  </div>
                  {step.output && (
                    <p className="mt-2 text-xs text-slate-600 ml-10 break-words">{step.output}</p>
                  )}
                  {/* Prereq resource details */}
                  {step.details && step.details.length > 0 && (
                    <div className="mt-3 ml-10 grid grid-cols-1 sm:grid-cols-3 gap-2">
                      {step.details.map((d, di) => (
                        <div key={di} className="flex items-start gap-2 p-2 rounded-md bg-white/70 border border-slate-100">
                          <span className={`mt-0.5 w-4 h-4 rounded-full flex items-center justify-center text-[9px] font-bold flex-shrink-0 ${
                            d.action === 'info' ? 'bg-amber-400 text-white' :
                            d.exists ? (d.action === 'created' ? 'bg-blue-500 text-white' : 'bg-green-500 text-white') : 'bg-red-400 text-white'
                          }`}>
                            {d.action === 'info' ? 'i' : d.exists ? (d.action === 'created' ? '+' : '\u2713') : '\u00d7'}
                          </span>
                          <div className="min-w-0">
                            <p className="text-[11px] font-medium text-slate-700 capitalize">{d.label}</p>
                            <p className="text-[10px] text-slate-500 font-mono truncate" title={d.name}>{d.name}</p>
                            {d.action && d.action !== 'exists' && d.action !== 'missing' && (
                              <span className={`inline-block mt-0.5 text-[9px] font-medium px-1.5 py-0.5 rounded ${
                                d.action === 'created' ? 'bg-blue-100 text-blue-700' :
                                d.action === 'info' ? 'bg-amber-100 text-amber-700' :
                                d.action === 'error' ? 'bg-red-100 text-red-700' :
                                'bg-slate-100 text-slate-600'
                              }`}>{d.action === 'info' ? 'optional' : d.action}</span>
                            )}
                            {d.url && <p className="text-[9px] text-slate-400 truncate" title={d.url}>{d.url}</p>}
                            {d.note && <p className="text-[9px] text-amber-600 truncate" title={d.note}>{d.note}</p>}
                            {d.error && <p className="text-[9px] text-red-500 truncate" title={d.error}>{d.error}</p>}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Cost Breakdown - per run */}
            {runCosts.some(c => c !== null) && (
              <div className="mt-6 space-y-4">
                {(() => {
                  const totalCost = runCosts.reduce((sum, c) => sum + (c?.total_cost_usd || 0), 0)
                  const totalDuration = runCosts.reduce((sum, c) => sum + (c?.duration_ms || 0), 0)
                  return (
                    <div className="p-4 bg-gradient-to-r from-orange-50 to-amber-50 rounded-lg border border-orange-200">
                      <div className="flex items-center justify-between">
                        <h4 className="text-sm font-bold text-orange-800">Pipeline Total</h4>
                        <div className="flex items-center gap-6">
                          <div className="text-right">
                            <p className="text-[10px] text-orange-600 uppercase">Total Cost</p>
                            <p className="text-xl font-bold text-orange-700">{formatCost(totalCost)}</p>
                          </div>
                          <div className="text-right">
                            <p className="text-[10px] text-orange-600 uppercase">Total Duration</p>
                            <p className="text-xl font-bold text-orange-700">{formatDuration(totalDuration)}</p>
                          </div>
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
                      <h4 className="text-sm font-bold text-slate-700 mb-3">{labels[ci]} (SF {e2eSF})</h4>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">VM Compute</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.vm_cost_usd)}</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">DBU Cost</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.dbu_cost_usd)}</p>
                          <p className="text-[9px] text-slate-400">{cost.total_dbus} DBUs @ {formatCost(cost.dbu_list_price)}/DBU</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">Local SSD</p>
                          <p className="text-base font-bold text-slate-800">{formatCost(cost.ssd_cost_usd)}</p>
                          <p className="text-[9px] text-slate-400">{cost.cluster.local_ssd_count} SSD(s)</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-orange-200 bg-orange-50">
                          <p className="text-[10px] text-orange-600 uppercase font-semibold">Total</p>
                          <p className="text-base font-bold text-orange-700">{formatCost(cost.total_cost_usd)}</p>
                        </div>
                        <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                          <p className="text-[10px] text-slate-500 uppercase">Duration</p>
                          <p className="text-sm font-mono text-slate-700">{formatDuration(cost.duration_ms)}</p>
                          <p className="text-[9px] text-slate-400">{cost.cluster.worker_node_type} x{cost.cluster.num_workers}</p>
                        </div>
                      </div>
                      {cost.run_page_url && (
                        <a href={cost.run_page_url} target="_blank" rel="noopener noreferrer" className="inline-block mt-2 text-xs text-orange-600 hover:text-orange-700 underline">View Run</a>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </Section>
      )}

      {/* ── Manual Mode ──────────────────────────────────────────────── */}
      {mode === 'manual' && (
      <>
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
                          s.status === 'info' ? 'bg-slate-100 text-slate-600' :
                          s.status === 'skipped' ? 'bg-amber-100 text-amber-700' :
                          s.status === 'missing' ? 'bg-amber-100 text-amber-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {s.status === 'info' ? 'OPTIONAL' : s.status.toUpperCase()}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-slate-500 max-w-xs">
                        <div className="truncate">{s.note || s.error || s.sa_email || s.url || '--'}</div>
                        {s.step === 'external_location' && (s.status === 'skipped' || s.status === 'error') && s.sa_email && s.gcs_bucket && (
                          <button
                            onClick={() => handleFixGcsIam(s.sa_email!, s.gcs_bucket!)}
                            disabled={fixingGcsIam}
                            className="mt-1 px-2 py-0.5 text-[10px] font-medium rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
                          >
                            {fixingGcsIam ? 'Fixing...' : 'Fix GCS Access'}
                          </button>
                        )}
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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 pt-4">
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
                { value: '17.3.x-scala2.13', label: '17.3 LTS (Scala 2.13)' },
                { value: '17.3.x-photon-scala2.13', label: '17.3 LTS Photon (Scala 2.13)' },
                { value: '17.2.x-scala2.13', label: '17.2 (Scala 2.13)' },
              ]}
            />
          </FormField>
          {computeMode === 'classic' && (
            <>
              <FormField label="Worker Node Type">
                <SelectInput value={workerNodeType} onChange={setWorkerNodeType} options={gcpNodeTypes} />
              </FormField>
              <FormField label="Driver Node Type">
                <SelectInput value={driverNodeType} onChange={setDriverNodeType} options={gcpNodeTypes} />
              </FormField>
              <FormField label="Number of Workers">
                <NumberInput value={numWorkers} onChange={setNumWorkers} min={1} max={100} />
              </FormField>
              <FormField label="Local SSDs per Node">
                <NumberInput value={localSsds} onChange={setLocalSsds} min={0} max={8} />
              </FormField>
            </>
          )}
          {computeMode === 'serverless' && (
            <div className="lg:col-span-3">
              <div className="flex items-center gap-2 px-4 py-3 bg-purple-50 border border-purple-200 rounded-lg text-sm text-purple-700">
                <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                Serverless compute auto-manages cluster resources. No node type or worker count configuration needed.
              </div>
            </div>
          )}
          <div className="lg:col-span-3">
            <FormField label="JAR Library Path" hint={`Default: ${effectiveJarPath}`}>
              <TextInput value={jarPath} onChange={setJarPath} placeholder={effectiveJarPath} />
            </FormField>
          </div>
          <div className="flex gap-6">
            {computeMode === 'classic' && <Checkbox checked={photon} onChange={setPhoton} label="Photon Enabled" />}
            <Checkbox checked={logDetailedStats} onChange={setLogDetailedStats} label="Log Detailed Stats" />
          </div>
        </div>

      </Section>
      )}

      {/* Pipeline Run Section - visible only when prerequisites are ready */}
      {prereqCheck?.status === 'ready' && (
      <Section title="3. Run Pipeline" defaultOpen={true} colorScheme="blue">
        <div className="pt-4">
          {/* Pipeline info and controls */}
          <div className="flex flex-wrap items-end gap-4 mb-6">
            <div className="text-sm text-slate-600">
              <span className="font-semibold">3-Step Pipeline:</span> Batch Load &rarr; Incremental Batch 2 &rarr; Incremental Batch 3 (SF {scaleFactor})
            </div>
            <ActionButton
              label={manualRunning ? 'Running...' : 'Run Pipeline'}
              onClick={handleManualPipeline}
              loading={manualRunning}
              disabled={manualRunning}
            />
            {manualRunning && manualRunId && (
              <button
                onClick={handleCancelManualPipeline}
                disabled={manualCancelling}
                className="px-4 py-2 text-sm font-medium rounded-md border border-red-300 text-red-600 hover:bg-red-50 disabled:opacity-50 transition-all"
              >
                {manualCancelling ? 'Cancelling...' : 'Cancel Run'}
              </button>
            )}
          </div>

          {/* Step Progress Cards */}
          <div className="space-y-3">
            {manualSteps.map((step, i) => (
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
                    <span className="text-sm font-semibold text-slate-700">Step {i + 1}: {step.name}</span>
                  </div>
                  <div className="flex items-center gap-3">
                    {step.duration_ms !== null && (
                      <span className="text-xs font-mono text-slate-500">{formatDuration(step.duration_ms)}</span>
                    )}
                    {manualCosts[i] && (
                      <span className="text-xs font-mono text-orange-600">{formatCost(manualCosts[i]!.total_cost_usd)}</span>
                    )}
                    {step.link && (
                      <a
                        href={step.link}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-orange-600 hover:text-orange-700 underline"
                      >
                        View Run
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
                  </div>
                </div>
                {step.output && (
                  <p className="mt-2 text-xs text-slate-600 ml-10 break-words">{step.output}</p>
                )}
              </div>
            ))}
          </div>

          {/* Cost Breakdown - per step */}
          {manualCosts.some(c => c !== null) && (
            <div className="mt-6 space-y-4">
              {(() => {
                const totalCost = manualCosts.reduce((sum, c) => sum + (c?.total_cost_usd || 0), 0)
                const totalDuration = manualCosts.reduce((sum, c) => sum + (c?.duration_ms || 0), 0)
                return (
                  <div className="p-4 bg-gradient-to-r from-orange-50 to-amber-50 rounded-lg border border-orange-200">
                    <div className="flex items-center justify-between">
                      <h4 className="text-sm font-bold text-orange-800">Pipeline Total</h4>
                      <div className="flex items-center gap-6">
                        <div className="text-right">
                          <p className="text-[10px] text-orange-600 uppercase">Total Cost</p>
                          <p className="text-xl font-bold text-orange-700">{formatCost(totalCost)}</p>
                        </div>
                        <div className="text-right">
                          <p className="text-[10px] text-orange-600 uppercase">Total Duration</p>
                          <p className="text-xl font-bold text-orange-700">{formatDuration(totalDuration)}</p>
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })()}
              {manualCosts.map((cost, ci) => {
                if (!cost) return null
                const labels = ['Batch Load', 'Incremental Batch 2', 'Incremental Batch 3']
                return (
                  <div key={ci} className="p-4 bg-gradient-to-r from-slate-50 to-blue-50 rounded-lg border border-slate-200">
                    <h4 className="text-sm font-bold text-slate-700 mb-3">{labels[ci]} (SF {scaleFactor})</h4>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">VM Compute</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(cost.vm_cost_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">DBU Cost</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(cost.dbu_cost_usd)}</p>
                        <p className="text-[9px] text-slate-400">{cost.total_dbus} DBUs @ {formatCost(cost.dbu_list_price)}/DBU</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">Local SSD</p>
                        <p className="text-base font-bold text-slate-800">{formatCost(cost.ssd_cost_usd)}</p>
                        <p className="text-[9px] text-slate-400">{cost.cluster.local_ssd_count} SSD(s)</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-orange-200 bg-orange-50">
                        <p className="text-[10px] text-orange-600 uppercase font-semibold">Total</p>
                        <p className="text-base font-bold text-orange-700">{formatCost(cost.total_cost_usd)}</p>
                      </div>
                      <div className="bg-white rounded-lg p-2.5 border border-slate-100">
                        <p className="text-[10px] text-slate-500 uppercase">Duration</p>
                        <p className="text-sm font-mono text-slate-700">{formatDuration(cost.duration_ms)}</p>
                        <p className="text-[9px] text-slate-400">{cost.cluster.worker_node_type} x{cost.cluster.num_workers}</p>
                      </div>
                    </div>
                    {cost.run_page_url && (
                      <a href={cost.run_page_url} target="_blank" rel="noopener noreferrer" className="inline-block mt-2 text-xs text-orange-600 hover:text-orange-700 underline">View Run</a>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </Section>
      )}
      </>
      )}

      {/* Run History */}
      <RunHistorySection platform="databricks" />
    </div>
  )
}
