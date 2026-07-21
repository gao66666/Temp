import { useEffect, useState } from 'react'
import type { Keyframe } from '../domain/media'

function waitForEvent(element: HTMLVideoElement, eventName: 'loadeddata' | 'loadedmetadata' | 'seeked') {
  return new Promise<void>((resolve, reject) => {
    const onSuccess = () => {
      cleanup()
      resolve()
    }
    const onError = () => {
      cleanup()
      reject(new Error('无法读取视频画面'))
    }
    const cleanup = () => {
      element.removeEventListener(eventName, onSuccess)
      element.removeEventListener('error', onError)
    }

    element.addEventListener(eventName, onSuccess, { once: true })
    element.addEventListener('error', onError, { once: true })
  })
}

export function useKeyframeThumbnails(sourceUrl: string, frames: Keyframe[]) {
  const [thumbnails, setThumbnails] = useState<Record<string, string>>({})
  const [isGenerating, setIsGenerating] = useState(false)

  useEffect(() => {
    let cancelled = false

    const provided = Object.fromEntries(
      frames.filter((frame) => frame.thumbnailUrl).map((frame) => [frame.id, frame.thumbnailUrl!]),
    )
    setThumbnails(provided)

    const missingFrames = frames.filter((frame) => !frame.thumbnailUrl)
    if (!sourceUrl || missingFrames.length === 0) return

    async function generate() {
      setIsGenerating(true)
      const probe = document.createElement('video')
      probe.preload = 'auto'
      probe.muted = true
      probe.playsInline = true
      probe.src = sourceUrl

      try {
        probe.load()
        await waitForEvent(probe, 'loadedmetadata')
        if (probe.readyState < HTMLMediaElement.HAVE_CURRENT_DATA) {
          await waitForEvent(probe, 'loadeddata')
        }

        const canvas = document.createElement('canvas')
        canvas.width = 320
        canvas.height = 180
        const context = canvas.getContext('2d')
        if (!context) throw new Error('Canvas 不可用')

        for (const frame of missingFrames) {
          if (cancelled) return

          const targetSeconds = Math.min(
            frame.timestampMs / 1000,
            Math.max(0, probe.duration - 0.05),
          )

          if (Math.abs(probe.currentTime - targetSeconds) > 0.01) {
            probe.currentTime = targetSeconds
            await waitForEvent(probe, 'seeked')
          }

          context.fillStyle = '#090a0b'
          context.fillRect(0, 0, canvas.width, canvas.height)

          const videoRatio = probe.videoWidth / probe.videoHeight
          const canvasRatio = canvas.width / canvas.height
          let drawWidth = canvas.width
          let drawHeight = canvas.height
          let x = 0
          let y = 0

          if (videoRatio > canvasRatio) {
            drawHeight = canvas.width / videoRatio
            y = (canvas.height - drawHeight) / 2
          } else {
            drawWidth = canvas.height * videoRatio
            x = (canvas.width - drawWidth) / 2
          }

          context.drawImage(probe, x, y, drawWidth, drawHeight)
          const thumbnail = canvas.toDataURL('image/jpeg', 0.78)
          setThumbnails((current) => ({ ...current, [frame.id]: thumbnail }))
        }
      } catch (error) {
        console.warn('Mock 缩略图生成失败', error)
      } finally {
        if (!cancelled) setIsGenerating(false)
        probe.removeAttribute('src')
        probe.load()
      }
    }

    void generate()
    return () => {
      cancelled = true
    }
  }, [frames, sourceUrl])

  return { thumbnails, isGenerating }
}
