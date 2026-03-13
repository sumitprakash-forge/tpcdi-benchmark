import { useState, useEffect, useCallback, useRef } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput, TextArea, Checkbox } from './FormField'
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

export default function DatabricksTab() {
  // Setup state
  const [catalogName, setCatalogName] = useState('gcp_tpcdi_classic')
  const [storageRoot, setStorageRoot] = useState('gs://')
  const [saEmail, setSaEmail] = useState('')
  const [saKeyJson, setSaKeyJson] = useState('')

  // Benchmark config state
  const [loadType, setLoadType] = useState('batch')
  const [scaleFactor, setScaleFactor] = useState('10')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
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
  const sizingSuggestion = (() => {
    const sf = parseInt(scaleFactor)
    if (loadType === 'batch') {
      if (sf >= 10000) return 'Recommended: Driver n2d-standard-8, Worker n2d-standard-16, 6 workers'
      if (sf >= 1000) return 'Recommended: Driver n2d-standard-8, Worker n2d-standard-8, 5 workers'
      return null
    } else {
      if (sf >= 10000) return 'Recommended: Driver n2d-standard-8, Worker n2d-standard-8, 5 workers'
      if (sf >= 1000) return 'Recommended: Driver n2d-standard-8, Worker n2d-standard-8, 3 workers'
      return null
    }
  })()

  // Run state
  const [jobId, setJobId] = useState<number | null>(null)
  const [runId, setRunId] = useState<number | null>(null)
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null)

  // Loading states
  const [loadingCred, setLoadingCred] = useState(false)
  const [loadingCatalog, setLoadingCatalog] = useState(false)
  const [loadingExtLoc, setLoadingExtLoc] = useState(false)
  const [loadingSchema, setLoadingSchema] = useState(false)
  const [loadingVolume, setLoadingVolume] = useState(false)
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

  // ── Setup Actions ────────────────────────────────────────────────────────

  async function handleCreateCredential() {
    setLoadingCred(true)
    try {
      const result = await apiCall('/api/databricks/create-storage-credential', {
        method: 'POST',
        body: JSON.stringify({ name: `${catalogName}_cred`, sa_email: saEmail, sa_key_json: saKeyJson }),
      })
      addToast(`Storage credential "${result.name}" created successfully`, 'success')
    } catch (e: any) {
      addToast(`Failed to create credential: ${e.message}`, 'error')
    } finally {
      setLoadingCred(false)
    }
  }

  async function handleCreateCatalog() {
    setLoadingCatalog(true)
    try {
      const result = await apiCall('/api/databricks/create-catalog', {
        method: 'POST',
        body: JSON.stringify({ name: catalogName, storage_root: storageRoot }),
      })
      addToast(`Catalog "${result.name}" created successfully`, 'success')
    } catch (e: any) {
      addToast(`Failed to create catalog: ${e.message}`, 'error')
    } finally {
      setLoadingCatalog(false)
    }
  }

  async function handleCreateExternalLocation() {
    setLoadingExtLoc(true)
    try {
      const result = await apiCall('/api/databricks/create-external-location', {
        method: 'POST',
        body: JSON.stringify({
          name: `${catalogName}_raw_data`,
          url: rawDataPath,
          credential_name: `${catalogName}_cred`,
        }),
      })
      addToast(`External location "${result.name}" created successfully`, 'success')
    } catch (e: any) {
      addToast(`Failed to create external location: ${e.message}`, 'error')
    } finally {
      setLoadingExtLoc(false)
    }
  }

  async function handleCreateSchema() {
    setLoadingSchema(true)
    try {
      const result = await apiCall('/api/databricks/create-schema', {
        method: 'POST',
        body: JSON.stringify({ catalog: catalogName, name: targetSchema }),
      })
      addToast(`Schema "${result.full_name}" created successfully`, 'success')
    } catch (e: any) {
      addToast(`Failed to create schema: ${e.message}`, 'error')
    } finally {
      setLoadingSchema(false)
    }
  }

  async function handleCreateVolume() {
    setLoadingVolume(true)
    try {
      // First ensure schema exists
      const result = await apiCall('/api/databricks/create-volume', {
        method: 'POST',
        body: JSON.stringify({ catalog: catalogName, schema_name: targetSchema, name: 'libs' }),
      })
      addToast(`Volume "${result.full_name}" created successfully`, 'success')
    } catch (e: any) {
      addToast(`Failed to create volume: ${e.message}`, 'error')
    } finally {
      setLoadingVolume(false)
    }
  }

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
    } catch (e: any) {
      addToast(`Failed to start benchmark: ${e.message}`, 'error')
    } finally {
      setLoadingRun(false)
    }
  }

  // ── Status Polling ───────────────────────────────────────────────────────

  const pollStatus = useCallback(async () => {
    if (!runId) return
    try {
      const status = await apiCall(`/api/databricks/job-status/${runId}`)
      setJobStatus(status)
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

  // Stop polling when terminal state is reached
  const isTerminal =
    jobStatus?.state === 'TERMINATED' ||
    jobStatus?.state === 'SKIPPED' ||
    jobStatus?.state === 'INTERNAL_ERROR'

  useEffect(() => {
    if (isTerminal && runId) {
      // One final poll happened, no need to continue
    }
  }, [isTerminal, runId])

  function formatDuration(ms: number | null) {
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

      {/* Setup Section */}
      <Section title="1. Setup - Create Catalog & Storage" defaultOpen={true}>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-4">
          <FormField label="Catalog Name">
            <TextInput value={catalogName} onChange={setCatalogName} placeholder="gcp_tpcdi_classic" />
          </FormField>
          <FormField label="Root Storage Location">
            <TextInput value={storageRoot} onChange={setStorageRoot} placeholder="gs://bucket/path" />
          </FormField>
          <FormField label="GCP Service Account Email">
            <TextInput value={saEmail} onChange={setSaEmail} placeholder="sa-name@project.iam.gserviceaccount.com" />
          </FormField>
          <div className="md:col-span-2">
            <FormField label="GCP Service Account Key (JSON)" hint="Paste the full SA key JSON content">
              <TextArea
                value={saKeyJson}
                onChange={setSaKeyJson}
                placeholder='{"type": "service_account", "project_id": "...", ...}'
                rows={5}
              />
            </FormField>
          </div>
        </div>

        <div className="flex flex-wrap gap-3 mt-5 pt-4 border-t border-slate-100">
          <ActionButton label="Create Storage Credential" onClick={handleCreateCredential} loading={loadingCred} />
          <ActionButton label="Create Catalog" onClick={handleCreateCatalog} loading={loadingCatalog} />
          <ActionButton
            label="Create External Location"
            onClick={handleCreateExternalLocation}
            loading={loadingExtLoc}
          />
          <ActionButton label="Create Schema" onClick={handleCreateSchema} loading={loadingSchema} variant="secondary" />
          <ActionButton label="Create Volume" onClick={handleCreateVolume} loading={loadingVolume} variant="secondary" />
        </div>
      </Section>

      {/* Benchmark Configuration Section */}
      <Section title="2. Benchmark Configuration">
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
                { value: '5000', label: '5000' },
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
          <FormField label="Raw Data Path">
            <TextInput value={rawDataPath} onChange={setRawDataPath} placeholder="gs://bucket/tpcdi" />
          </FormField>
          <FormField label="Target Catalog">
            <TextInput value={targetCatalog} onChange={setTargetCatalog} placeholder="gcp_tpcdi_classic" />
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

        {/* Sizing Suggestion */}
        {sizingSuggestion && (
          <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg">
            <p className="text-sm text-amber-800">
              <span className="font-semibold">Sizing suggestion for {loadType} SF={scaleFactor}:</span>{' '}
              {sizingSuggestion}
            </p>
          </div>
        )}
      </Section>

      {/* Run Section */}
      <Section title="3. Run Benchmark" defaultOpen={true}>
        <div className="pt-4">
          <div className="flex flex-wrap gap-3 items-center">
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
              </span>
            )}
          </div>

          {/* Job Status Display */}
          {jobStatus && (
            <div className="mt-5 p-4 bg-slate-50 rounded-lg border border-slate-200">
              <h4 className="text-sm font-semibold text-slate-700 mb-3">Run Status</h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-xs text-slate-500 uppercase">State</p>
                  <p className="text-sm font-medium">
                    <span
                      className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                        jobStatus.state === 'TERMINATED' && jobStatus.result_state === 'SUCCESS'
                          ? 'bg-green-100 text-green-700'
                          : jobStatus.state === 'TERMINATED'
                          ? 'bg-red-100 text-red-700'
                          : jobStatus.state === 'RUNNING'
                          ? 'bg-blue-100 text-blue-700'
                          : 'bg-yellow-100 text-yellow-700'
                      }`}
                    >
                      {jobStatus.result_state || jobStatus.state}
                    </span>
                  </p>
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
                  <p className="text-sm text-slate-600 mt-1">{jobStatus.state_message}</p>
                </div>
              )}
              {jobStatus.run_page_url && (
                <div className="mt-3">
                  <a
                    href={jobStatus.run_page_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-orange-600 hover:text-orange-700 underline"
                  >
                    View Run in Databricks
                  </a>
                </div>
              )}
              {!isTerminal && runId && (
                <p className="mt-2 text-xs text-slate-400">Auto-refreshing every 10 seconds...</p>
              )}
            </div>
          )}
        </div>
      </Section>
    </div>
  )
}
