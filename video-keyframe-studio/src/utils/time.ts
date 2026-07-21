export function formatTime(milliseconds: number, withMilliseconds = false) {
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return '00:00'

  const totalSeconds = milliseconds / 1000
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = Math.floor(totalSeconds % 60)
  const fraction = Math.floor(milliseconds % 1000)

  const base = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
  return withMilliseconds ? `${base}.${String(fraction).padStart(3, '0')}` : base
}
