interface ActionButtonProps {
  label: string
  onClick: () => void
  loading?: boolean
  variant?: 'primary' | 'secondary' | 'danger'
  disabled?: boolean
}

export default function ActionButton({
  label,
  onClick,
  loading = false,
  variant = 'primary',
  disabled = false,
}: ActionButtonProps) {
  const base = 'px-4 py-2 text-sm font-medium rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-2'
  const variants = {
    primary: 'bg-orange-500 hover:bg-orange-600 text-white',
    secondary: 'bg-slate-600 hover:bg-slate-700 text-white',
    danger: 'bg-red-500 hover:bg-red-600 text-white',
  }

  return (
    <button
      onClick={onClick}
      disabled={loading || disabled}
      className={`${base} ${variants[variant]}`}
    >
      {loading && (
        <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
          <path
            className="opacity-75"
            fill="currentColor"
            d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
          />
        </svg>
      )}
      {label}
    </button>
  )
}
