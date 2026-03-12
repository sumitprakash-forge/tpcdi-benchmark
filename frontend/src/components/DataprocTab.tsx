import { useState } from 'react'
import Section from './Section'
import ActionButton from './ActionButton'
import FormField, { TextInput, NumberInput, SelectInput } from './FormField'

export default function DataprocTab() {
  const [projectId, setProjectId] = useState('')
  const [clusterName, setClusterName] = useState('')
  const [region, setRegion] = useState('us-central1')
  const [scaleFactor, setScaleFactor] = useState('10')
  const [rawDataPath, setRawDataPath] = useState('gs://sumit_prakash_gcs/tpcdi')
  const [targetCatalog, setTargetCatalog] = useState('gcp_tpcdi_classic')
  const [numWorkers, setNumWorkers] = useState(3)
  const [machineType, setMachineType] = useState('n2-highmem-16')

  function handleSubmit() {
    // Placeholder
  }

  return (
    <div>
      <Section title="Dataproc Benchmark Configuration" defaultOpen={true}>
        <div className="pt-4">
          <div className="flex items-center gap-2 mb-5 p-3 bg-blue-50 border border-blue-200 rounded-lg">
            <svg className="w-5 h-5 text-blue-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <p className="text-sm text-blue-700">
              Configure and run TPC-DI benchmark on Google Cloud Dataproc
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <FormField label="GCP Project ID">
              <TextInput value={projectId} onChange={setProjectId} placeholder="my-gcp-project" />
            </FormField>
            <FormField label="Dataproc Cluster Name">
              <TextInput value={clusterName} onChange={setClusterName} placeholder="tpcdi-cluster" />
            </FormField>
            <FormField label="Region">
              <SelectInput
                value={region}
                onChange={setRegion}
                options={[
                  { value: 'us-central1', label: 'us-central1' },
                  { value: 'us-east1', label: 'us-east1' },
                  { value: 'us-west1', label: 'us-west1' },
                  { value: 'us-west2', label: 'us-west2' },
                  { value: 'europe-west1', label: 'europe-west1' },
                  { value: 'europe-west2', label: 'europe-west2' },
                  { value: 'asia-east1', label: 'asia-east1' },
                  { value: 'asia-southeast1', label: 'asia-southeast1' },
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
                ]}
              />
            </FormField>
            <FormField label="Raw Data Path">
              <TextInput value={rawDataPath} onChange={setRawDataPath} placeholder="gs://bucket/tpcdi" />
            </FormField>
            <FormField label="Target Catalog">
              <TextInput value={targetCatalog} onChange={setTargetCatalog} placeholder="catalog_name" />
            </FormField>
            <FormField label="Number of Workers">
              <NumberInput value={numWorkers} onChange={setNumWorkers} min={1} max={100} />
            </FormField>
            <FormField label="Machine Type">
              <SelectInput
                value={machineType}
                onChange={setMachineType}
                options={[
                  { value: 'n2-highmem-16', label: 'n2-highmem-16' },
                  { value: 'n2-standard-16', label: 'n2-standard-16' },
                  { value: 'n2-highmem-32', label: 'n2-highmem-32' },
                  { value: 'n2-standard-32', label: 'n2-standard-32' },
                  { value: 'n2-highmem-8', label: 'n2-highmem-8' },
                  { value: 'n2-standard-8', label: 'n2-standard-8' },
                ]}
              />
            </FormField>
          </div>

          <div className="mt-5 pt-4 border-t border-slate-100">
            <ActionButton label="Submit Dataproc Job" onClick={handleSubmit} variant="secondary" disabled />
            <p className="mt-3 text-sm text-slate-500 italic">Coming soon - Dataproc integration is under development.</p>
          </div>
        </div>
      </Section>
    </div>
  )
}
