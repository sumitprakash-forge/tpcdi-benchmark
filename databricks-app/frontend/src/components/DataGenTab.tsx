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

export default function DataGenTab() {
  // Config state
  const [scaleFactor, setScaleFactor] = useState('100')
  const [outputPath, setOutputPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [cloud, setCloud] = useState('GCP')
  const [sparkVersion, setSparkVersion] = useState('16.4.x-scala2.12')
  const [nodeType, setNodeType] = useState('n2-highmem-16')
  const [numWorkers, setNumWorkers] = useState(0)
  const [photon, setPhoton] = useState(false)
  const [localSsds, setLocalSsds] = useState(1)
  const [localGenPath, setLocalGenPath] = useState('/local_disk0')
  const [uploadThreads, setUploadThreads] = useState(8)
  const [skipIfExists, setSkipIfExists] = useState(true)
  const [notebookPath, setNotebookPath] = useState(
    '/Repos/sumit.prakash@databricks.com/tpcdi-benchmark/databricks/generate_tpcdi_data_notebook'
  )

  // Run state
  const [jobId, setJobId] = useState<number | null>(null)
  const [runId, setRunId] = useState<number | null>(null)
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null)

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

  // Estimated data size
  const estimatedSize = (() => {
    const sf = parseInt(scaleFactor)
    if (sf <= 10) return '~1 GB'
    if (sf <= 100) return '~10 GB'
    if (sf <= 1000) return '~100 GB'
    return `~${Math.round(sf / 10)} GB`
  })()

  async function handleCreateJob() {
    setLoadingJob(true)
    try {
      const result = await apiCall('/api/databricks/create-datagen-job', {
        method: 'POST',
        body: JSON.stringify({
          job_name: `TPC-DI DataGen SF${scaleFactor}`,
          scale_factor: parseInt(scaleFactor),
          output_path: outputPath,
          spark_version: sparkVersion,
          node_type: nodeType,
          num_workers: numWorkers,
          photon,
          local_ssds: localSsds,
          cloud,
          local_gen_path: localGenPath,
          upload_threads: uploadThreads,
          skip_if_exists: skipIfExists,
          notebook_path: notebookPath,
          job_id: jobId,
        }),
      })
      setJobId(result.job_id)
      addToast(`DataGen job ${result.action}: ID ${result.job_id}`, 'success')
    } catch (e: any) {
      addToast(`Failed to create/update datagen job: ${e.message}`, 'error')
    } finally {
      setLoadingJob(false)
    }
  }

  async function handleRunDataGen() {
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
      setJobStatus(null)
      addToast(`Data generation started: Run ID ${result.run_id}`, 'success')
    } catch (e: any) {
      addToast(`Failed to start data generation: ${e.message}`, 'error')
    } finally {
      setLoadingRun(false)
    }
  }

  // Status polling
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

  const isTerminal =
    jobStatus?.state === 'TERMINATED' ||
    jobStatus?.state === 'SKIPPED' ||
    jobStatus?.state === 'INTERNAL_ERROR'

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

      <div className="mb-4 p-4 bg-emerald-50 border border-emerald-200 rounded-lg">
        <h3 className="text-sm font-semibold text-emerald-800">TPC-DI Data Generation</h3>
        <p className="text-sm text-emerald-700 mt-1">
          Generate TPC-DI benchmark raw data using the official DIGen tool. Data is generated on the driver node
          and uploaded to the output path (DBFS, UC Volume, or GCS). Requires Java 7+ and TPC-DI Tools v1.1.0
          in the repo's <code className="bg-emerald-100 px-1 rounded">tools/datagen/</code> directory.
        </p>
      </div>

      {/* Configuration Section */}
      <Section title="1. Data Generation Configuration" defaultOpen={true}>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 pt-4">
          <FormField label="Scale Factor" hint={`Estimated data size: ${estimatedSize}`}>
            <SelectInput
              value={scaleFactor}
              onChange={setScaleFactor}
              options={[
                { value: '10', label: '10 (~1 GB)' },
                { value: '100', label: '100 (~10 GB)' },
                { value: '1000', label: '1000 (~100 GB)' },
                { value: '10000', label: '10000 (~1 TB)' },
              ]}
            />
          </FormField>
          <FormField label="Output Path" hint="DBFS (dbfs:/...), UC Volume (/Volumes/...), GCS (gs://...), or local">
            <TextInput value={outputPath} onChange={setOutputPath} placeholder="gs://bucket/tpcdi" />
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
          <FormField label="Local Gen Path" hint="Driver local path for temp files. Use local SSD for large SF.">
            <TextInput value={localGenPath} onChange={setLocalGenPath} placeholder="/local_disk0" />
          </FormField>
          <FormField label="Upload Threads" hint="Parallel threads for uploading files to destination">
            <NumberInput value={uploadThreads} onChange={setUploadThreads} min={1} max={32} />
          </FormField>
          <div className="flex items-end">
            <Checkbox checked={skipIfExists} onChange={setSkipIfExists} label="Skip if output exists" />
          </div>
        </div>
      </Section>

      {/* Cluster Configuration Section */}
      <Section title="2. Cluster Configuration">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 pt-4">
          <FormField label="Spark Version">
            <SelectInput
              value={sparkVersion}
              onChange={setSparkVersion}
              options={[
                { value: '16.4.x-scala2.12', label: '16.4.x-scala2.12 (LTS)' },
                { value: '15.4.x-scala2.12', label: '15.4.x-scala2.12 (LTS)' },
              ]}
            />
          </FormField>
          <FormField label="Node Type" hint="Driver-only (0 workers) recommended for datagen">
            <TextInput value={nodeType} onChange={setNodeType} placeholder="n2-highmem-16" />
          </FormField>
          <FormField label="Number of Workers" hint="0 = driver-only (datagen is single-threaded Java)">
            <NumberInput value={numWorkers} onChange={setNumWorkers} min={0} max={100} />
          </FormField>
          <FormField label="# Local SSDs" hint="Number of local SSDs per node (375 GB each). 0 = none.">
            <NumberInput value={localSsds} onChange={setLocalSsds} min={0} max={8} />
          </FormField>
          <div className="flex items-end">
            <Checkbox checked={photon} onChange={setPhoton} label="Photon Enabled" />
          </div>
        </div>
      </Section>

      {/* Advanced Section */}
      <Section title="3. Advanced Settings">
        <div className="grid grid-cols-1 gap-4 pt-4">
          <FormField label="Datagen Notebook Path" hint="Workspace path to the generate_tpcdi_data_notebook">
            <TextInput value={notebookPath} onChange={setNotebookPath} placeholder="/Repos/..." />
          </FormField>
        </div>
      </Section>

      {/* Run Section */}
      <Section title="4. Run Data Generation" defaultOpen={true}>
        <div className="pt-4">
          <div className="flex flex-wrap gap-3 items-center">
            <ActionButton
              label={jobId ? 'Update DataGen Job' : 'Create DataGen Job'}
              onClick={handleCreateJob}
              loading={loadingJob}
            />
            <ActionButton
              label="Generate Data"
              onClick={handleRunDataGen}
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

          {/* Output path preview */}
          <div className="mt-3 p-3 bg-slate-50 rounded border border-slate-200">
            <p className="text-xs text-slate-500 uppercase mb-1">Output will be written to</p>
            <p className="text-sm font-mono text-slate-700">{outputPath}/sf={scaleFactor}</p>
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
                    className="text-sm text-emerald-600 hover:text-emerald-700 underline"
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
