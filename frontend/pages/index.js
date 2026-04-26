import { useEffect, useState } from 'react'

export default function Home() {
  const [status, setStatus] = useState('loading')

  useEffect(() => {
    fetch('http://localhost:8000/')
      .then((r) => r.json())
      .then((d) => setStatus(d.status))
      .catch(() => setStatus('unreachable'))
  }, [])

  return (
    <main className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="p-8 bg-white rounded shadow text-center">
        <h1 className="text-2xl font-semibold mb-4">Pulse Frontend</h1>
        <p className="text-sm text-gray-600 mb-2">Backend status: <strong>{status}</strong></p>
      </div>
    </main>
  )
}
