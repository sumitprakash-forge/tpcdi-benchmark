export type ComputeMode = 'serverless' | 'classic'

interface ComputeModeSelectorProps {
  mode: ComputeMode
  onChange: (mode: ComputeMode) => void
  platform: 'databricks' | 'dataproc'
}

const ServerlessIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 15a4.5 4.5 0 004.5 4.5H18a3.75 3.75 0 001.332-7.257 3 3 0 00-3.758-3.848 5.25 5.25 0 00-10.233 2.33A4.502 4.502 0 002.25 15z" />
  </svg>
)

const ClassicIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M5.25 14.25h13.5m-13.5 0a3 3 0 01-3-3m3 3a3 3 0 100 6h13.5a3 3 0 100-6m-16.5-3a3 3 0 013-3h13.5a3 3 0 013 3m-19.5 0a4.5 4.5 0 01.9-2.7L5.737 5.1a3.375 3.375 0 012.7-1.35h7.126c1.062 0 2.062.5 2.7 1.35l2.587 3.45a4.5 4.5 0 01.9 2.7m0 0a3 3 0 01-3 3m0 3h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008zm-3 6h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008z" />
  </svg>
)

const descriptions: Record<string, Record<ComputeMode, { title: string; subtitle: string; features: string[] }>> = {
  databricks: {
    serverless: {
      title: 'Serverless',
      subtitle: 'Zero infrastructure management',
      features: ['Auto-scaling', 'No cluster config', 'Pay per query', 'Instant start'],
    },
    classic: {
      title: 'Classic Cluster',
      subtitle: 'Full control over compute',
      features: ['Custom node types', 'GPU support', 'Local SSDs', 'Photon engine'],
    },
  },
  dataproc: {
    serverless: {
      title: 'Serverless Batch',
      subtitle: 'Dataproc Serverless (Batches API)',
      features: ['No cluster mgmt', 'Auto-scaling', 'Fast startup', 'Cost-efficient'],
    },
    classic: {
      title: 'Classic Cluster',
      subtitle: 'Managed Dataproc cluster',
      features: ['Custom machine types', 'Local SSDs', 'Metastore attach', 'Full control'],
    },
  },
}

export default function ComputeModeSelector({ mode, onChange, platform }: ComputeModeSelectorProps) {
  const options = descriptions[platform]

  return (
    <div className="mb-5">
      <label className="block text-sm font-semibold text-slate-700 mb-2">Compute Mode</label>
      <div className="grid grid-cols-2 gap-3">
        {(['classic', 'serverless'] as ComputeMode[]).map((opt) => {
          const selected = mode === opt
          const info = options[opt]
          return (
            <button
              key={opt}
              type="button"
              onClick={() => onChange(opt)}
              className={`relative flex flex-col items-start p-4 rounded-xl border-2 transition-all text-left ${
                selected
                  ? 'border-purple-500 bg-purple-50 shadow-sm ring-1 ring-purple-200'
                  : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'
              }`}
            >
              {/* Selection indicator */}
              <div className={`absolute top-3 right-3 w-5 h-5 rounded-full border-2 flex items-center justify-center transition-colors ${
                selected ? 'border-purple-500 bg-purple-500' : 'border-slate-300'
              }`}>
                {selected && (
                  <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                )}
              </div>

              {/* Icon + Title */}
              <div className="flex items-center gap-2 mb-1.5">
                <span className={selected ? 'text-purple-600' : 'text-slate-400'}>
                  {opt === 'serverless' ? <ServerlessIcon /> : <ClassicIcon />}
                </span>
                <span className={`text-sm font-semibold ${selected ? 'text-purple-700' : 'text-slate-700'}`}>
                  {info.title}
                </span>
              </div>

              {/* Subtitle */}
              <p className={`text-[11px] mb-2 ${selected ? 'text-purple-500' : 'text-slate-400'}`}>
                {info.subtitle}
              </p>

              {/* Feature pills */}
              <div className="flex flex-wrap gap-1">
                {info.features.map((f) => (
                  <span
                    key={f}
                    className={`inline-block px-1.5 py-0.5 rounded text-[9px] font-medium ${
                      selected
                        ? 'bg-purple-100 text-purple-600'
                        : 'bg-slate-100 text-slate-500'
                    }`}
                  >
                    {f}
                  </span>
                ))}
              </div>
            </button>
          )
        })}
      </div>
    </div>
  )
}
