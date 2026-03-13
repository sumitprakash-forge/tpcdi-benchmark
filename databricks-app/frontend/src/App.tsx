import { useState } from 'react'
import DataGenTab from './components/DataGenTab'
import DatabricksTab from './components/DatabricksTab'
import DataprocTab from './components/DataprocTab'
import ResultsTab from './components/ResultsTab'

type TabKey = 'datagen' | 'databricks' | 'dataproc' | 'results'

const tabs: { key: TabKey; label: string; color: string }[] = [
  { key: 'datagen', label: 'Data Generation', color: 'emerald' },
  { key: 'databricks', label: 'Databricks', color: 'orange' },
  { key: 'dataproc', label: 'Dataproc', color: 'blue' },
  { key: 'results', label: 'Results', color: 'purple' },
]

const activeColors: Record<string, string> = {
  emerald: 'bg-emerald-600 text-white',
  orange: 'bg-orange-500 text-white',
  blue: 'bg-blue-600 text-white',
  purple: 'bg-purple-600 text-white',
}

function App() {
  const [activeTab, setActiveTab] = useState<TabKey>('datagen')

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="bg-white border-b border-slate-200 shadow-sm">
        <div className="max-w-6xl mx-auto px-6 py-4">
          <h1 className="text-2xl font-bold text-slate-800 mb-4">TPC-DI Benchmark Runner</h1>
          <div className="flex gap-1">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`px-5 py-2.5 text-sm font-medium rounded-t-lg transition-colors ${
                  activeTab === tab.key
                    ? activeColors[tab.color]
                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-6">
        {activeTab === 'datagen' && <DataGenTab />}
        {activeTab === 'databricks' && <DatabricksTab />}
        {activeTab === 'dataproc' && <DataprocTab />}
        {activeTab === 'results' && <ResultsTab />}
      </main>
    </div>
  )
}

export default App
