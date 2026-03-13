import { useState, type ReactNode } from 'react'

type ColorScheme = 'slate' | 'emerald' | 'amber' | 'blue'

const colorMap: Record<ColorScheme, { border: string; header: string; headerHover: string; title: string; divider: string; bg: string }> = {
  slate: {
    border: 'border-slate-200',
    header: '',
    headerHover: 'hover:bg-slate-50',
    title: 'text-slate-700',
    divider: 'border-slate-100',
    bg: 'bg-white',
  },
  emerald: {
    border: 'border-emerald-200',
    header: 'bg-emerald-50',
    headerHover: 'hover:bg-emerald-100',
    title: 'text-emerald-800',
    divider: 'border-emerald-100',
    bg: 'bg-emerald-50/30',
  },
  amber: {
    border: 'border-amber-200',
    header: 'bg-amber-50',
    headerHover: 'hover:bg-amber-100',
    title: 'text-amber-800',
    divider: 'border-amber-100',
    bg: 'bg-amber-50/30',
  },
  blue: {
    border: 'border-blue-200',
    header: 'bg-blue-50',
    headerHover: 'hover:bg-blue-100',
    title: 'text-blue-800',
    divider: 'border-blue-100',
    bg: 'bg-blue-50/30',
  },
}

interface SectionProps {
  title: string
  defaultOpen?: boolean
  colorScheme?: ColorScheme
  children: ReactNode
}

export default function Section({ title, defaultOpen = false, colorScheme = 'slate', children }: SectionProps) {
  const [open, setOpen] = useState(defaultOpen)
  const c = colorMap[colorScheme]

  return (
    <div className={`${c.bg} rounded-lg border ${c.border} shadow-sm mb-4`}>
      <button
        onClick={() => setOpen(!open)}
        className={`w-full flex items-center justify-between px-5 py-4 text-left ${c.header} ${c.headerHover} transition-colors rounded-lg`}
      >
        <span className={`text-base font-semibold ${c.title}`}>{title}</span>
        <svg
          className={`w-5 h-5 ${c.title} opacity-60 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && <div className={`px-5 pb-5 border-t ${c.divider}`}>{children}</div>}
    </div>
  )
}
