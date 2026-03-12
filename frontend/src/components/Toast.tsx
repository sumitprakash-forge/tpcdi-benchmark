import { useEffect } from 'react'

interface ToastProps {
  message: string
  type: 'success' | 'error' | 'info'
  onClose: () => void
}

export default function Toast({ message, type, onClose }: ToastProps) {
  useEffect(() => {
    const timer = setTimeout(onClose, 5000)
    return () => clearTimeout(timer)
  }, [onClose])

  const bgColor =
    type === 'success'
      ? 'bg-green-50 border-green-300 text-green-800'
      : type === 'error'
      ? 'bg-red-50 border-red-300 text-red-800'
      : 'bg-blue-50 border-blue-300 text-blue-800'

  return (
    <div className={`fixed top-4 right-4 z-50 max-w-md px-4 py-3 rounded-lg border shadow-lg ${bgColor}`}>
      <div className="flex items-start gap-2">
        <span className="text-sm flex-1">{message}</span>
        <button onClick={onClose} className="text-current opacity-60 hover:opacity-100 font-bold">
          x
        </button>
      </div>
    </div>
  )
}
