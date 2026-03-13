import { type ReactNode } from 'react'

interface FormFieldProps {
  label: string
  children: ReactNode
  hint?: string
}

export default function FormField({ label, children, hint }: FormFieldProps) {
  return (
    <div>
      <label className="block text-sm font-medium text-slate-700 mb-1">{label}</label>
      {children}
      {hint && <p className="mt-1 text-xs text-slate-400">{hint}</p>}
    </div>
  )
}

export function TextInput({
  value,
  onChange,
  placeholder,
}: {
  value: string
  onChange: (v: string) => void
  placeholder?: string
}) {
  return (
    <input
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="w-full px-3 py-2 text-sm border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-orange-400 focus:border-transparent bg-white"
    />
  )
}

export function NumberInput({
  value,
  onChange,
  min,
  max,
}: {
  value: number
  onChange: (v: number) => void
  min?: number
  max?: number
}) {
  return (
    <input
      type="number"
      value={value}
      onChange={(e) => onChange(parseInt(e.target.value) || 0)}
      min={min}
      max={max}
      className="w-full px-3 py-2 text-sm border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-orange-400 focus:border-transparent bg-white"
    />
  )
}

export function SelectInput({
  value,
  onChange,
  options,
}: {
  value: string
  onChange: (v: string) => void
  options: { value: string; label: string }[]
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full px-3 py-2 text-sm border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-orange-400 focus:border-transparent bg-white"
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  )
}

export function TextArea({
  value,
  onChange,
  placeholder,
  rows,
}: {
  value: string
  onChange: (v: string) => void
  placeholder?: string
  rows?: number
}) {
  return (
    <textarea
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      rows={rows || 4}
      className="w-full px-3 py-2 text-sm border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-orange-400 focus:border-transparent bg-white font-mono"
    />
  )
}

export function Checkbox({
  checked,
  onChange,
  label,
}: {
  checked: boolean
  onChange: (v: boolean) => void
  label: string
}) {
  return (
    <label className="flex items-center gap-2 cursor-pointer">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="w-4 h-4 text-orange-500 border-slate-300 rounded focus:ring-orange-400"
      />
      <span className="text-sm text-slate-700">{label}</span>
    </label>
  )
}
