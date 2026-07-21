import { useEffect, useMemo, useRef, useState } from 'react'
import type { Keyframe, MediaAsset } from '../domain/media'
import { useKeyframeThumbnails } from '../hooks/useKeyframeThumbnails'
import { formatTime } from '../utils/time'
import { KeyframeTimeline } from './KeyframeTimeline'

interface VideoWorkspaceProps {
  asset: MediaAsset
}

function createWindowFrames(durationMs: number, windowMs: number): Keyframe[] {
  const frames: Keyframe[] = []
  for (let timestampMs = 0; timestampMs < durationMs; timestampMs += windowMs) {
    frames.push({ id: `runtime-${timestampMs}`, timestampMs })
  }
  return frames
}

function findActiveFrame(frames: Keyframe[], currentTimeMs: number) {
  let low = 0
  let high = frames.length - 1

  while (low <= high) {
    const middle = Math.floor((low + high) / 2)
    if (frames[middle].timestampMs <= currentTimeMs) low = middle + 1
    else high = middle - 1
  }

  return frames[Math.max(0, high)]
}

export function VideoWorkspace({ asset }: VideoWorkspaceProps) {
  const videoRef = useRef<HTMLVideoElement>(null)
  const playerRef = useRef<HTMLDivElement>(null)
  const [currentTimeMs, setCurrentTimeMs] = useState(0)
  const [durationMs, setDurationMs] = useState(asset.keyframes.durationMs)
  const [isPlaying, setIsPlaying] = useState(false)
  const [isMuted, setIsMuted] = useState(false)
  const [isSeeking, setIsSeeking] = useState(false)
  const [selectedFrameId, setSelectedFrameId] = useState<string>()
  const [runtimeFrames, setRuntimeFrames] = useState<Keyframe[]>(asset.keyframes.frames)

  useEffect(() => {
    setCurrentTimeMs(0)
    setDurationMs(asset.keyframes.durationMs)
    setRuntimeFrames(asset.keyframes.frames)
    setIsPlaying(false)
    setSelectedFrameId(undefined)
  }, [asset])

  const frames = runtimeFrames.length ? runtimeFrames : asset.keyframes.frames
  const activeFrame = useMemo(
    () => findActiveFrame(frames, currentTimeMs),
    [currentTimeMs, frames],
  )
  const { thumbnails, isGenerating } = useKeyframeThumbnails(asset.source.playbackUrl, frames)

  const togglePlayback = async () => {
    const video = videoRef.current
    if (!video) return
    if (video.paused) {
      setSelectedFrameId(undefined)
      await video.play()
    }
    else video.pause()
  }

  const seekTo = (milliseconds: number, preserveSelection = false) => {
    const video = videoRef.current
    if (!video) return
    if (!preserveSelection) setSelectedFrameId(undefined)
    video.currentTime = Math.min(milliseconds / 1000, video.duration || Infinity)
    setCurrentTimeMs(milliseconds)
  }

  const seekToFrame = (frame: Keyframe) => {
    const video = videoRef.current
    if (!video) return
    video.pause()
    setSelectedFrameId(frame.id)
    seekTo(frame.timestampMs, true)
  }

  const stepFrame = (direction: -1 | 1) => {
    const currentIndex = Math.max(0, frames.findIndex((frame) => frame.id === activeFrame?.id))
    const nextIndex = Math.min(frames.length - 1, Math.max(0, currentIndex + direction))
    const nextFrame = frames[nextIndex]
    if (nextFrame) seekToFrame(nextFrame)
  }

  const handleLoadedMetadata = () => {
    const video = videoRef.current
    if (!video) return
    const actualDurationMs = video.duration * 1000
    setDurationMs(actualDurationMs)
    if (asset.keyframes.frames.length === 0) {
      setRuntimeFrames(createWindowFrames(actualDurationMs, asset.keyframes.windowMs))
    }
  }

  return (
    <main
      className="workspace"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.key === 'ArrowLeft') {
          event.preventDefault()
          stepFrame(-1)
        }
        if (event.key === 'ArrowRight') {
          event.preventDefault()
          stepFrame(1)
        }
        if (event.key === ' ') {
          event.preventDefault()
          void togglePlayback()
        }
      }}
    >
      <section className="player-card">
        <div className="player-stage" ref={playerRef} onDoubleClick={() => void playerRef.current?.requestFullscreen()}>
          <video
            ref={videoRef}
            src={asset.source.playbackUrl}
            playsInline
            preload="metadata"
            muted={isMuted}
            onLoadedMetadata={handleLoadedMetadata}
            onTimeUpdate={(event) => setCurrentTimeMs(event.currentTarget.currentTime * 1000)}
            onPlay={() => {
              setSelectedFrameId(undefined)
              setIsPlaying(true)
            }}
            onPause={() => setIsPlaying(false)}
            onSeeking={() => setIsSeeking(true)}
            onSeeked={() => setIsSeeking(false)}
            onEnded={() => setIsPlaying(false)}
          />

          {!isPlaying && (
            <button className="hero-play" onClick={() => void togglePlayback()} aria-label="播放视频">
              <PlayIcon />
            </button>
          )}
          {isSeeking && <div className="seek-indicator">定位中…</div>}
          <div className="stage-chip">
            <span className="source-dot" />
            {asset.source.kind === 'local' ? '本地素材' : '云端素材'}
          </div>
        </div>

        <div className="player-controls">
          <button className="icon-button primary" onClick={() => void togglePlayback()} aria-label={isPlaying ? '暂停' : '播放'}>
            {isPlaying ? <PauseIcon /> : <PlayIcon />}
          </button>
          <button className="icon-button" onClick={() => stepFrame(-1)} aria-label="上一关键帧"><SkipIcon flip /></button>
          <button className="icon-button" onClick={() => stepFrame(1)} aria-label="下一关键帧"><SkipIcon /></button>
          <span className="time-readout">{formatTime(currentTimeMs)} <em>/</em> {formatTime(durationMs)}</span>
          <input
            className="progress-slider"
            type="range"
            min={0}
            max={Math.max(1, durationMs)}
            step={10}
            value={Math.min(currentTimeMs, durationMs || 0)}
            aria-label="视频进度"
            style={{ '--progress': `${durationMs ? (currentTimeMs / durationMs) * 100 : 0}%` } as React.CSSProperties}
            onChange={(event) => seekTo(Number(event.currentTarget.value))}
          />
          <button className="icon-button" onClick={() => setIsMuted((muted) => !muted)} aria-label={isMuted ? '取消静音' : '静音'}>
            <VolumeIcon muted={isMuted} />
          </button>
          <button className="icon-button" onClick={() => void playerRef.current?.requestFullscreen()} aria-label="全屏">
            <FullscreenIcon />
          </button>
        </div>
      </section>

      <KeyframeTimeline
        frames={frames}
        currentFrameId={activeFrame?.id}
        selectedFrameId={selectedFrameId}
        thumbnails={thumbnails}
        isGenerating={isGenerating}
        windowMs={asset.keyframes.windowMs}
        onSeek={seekToFrame}
      />
    </main>
  )
}

function PlayIcon() {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m8 5 11 7-11 7V5Z" /></svg>
}

function PauseIcon() {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 5h3v14H7zm7 0h3v14h-3z" /></svg>
}

function SkipIcon({ flip = false }: { flip?: boolean }) {
  return <svg viewBox="0 0 24 24" aria-hidden="true" style={flip ? { transform: 'scaleX(-1)' } : undefined}><path d="m7 6 9 6-9 6V6Zm10 0h2v12h-2V6Z" /></svg>
}

function VolumeIcon({ muted }: { muted: boolean }) {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9v6h4l5 4V5L8 9H4Z" />{muted ? <path className="stroke" d="m17 9 4 4m0-4-4 4" /> : <path className="stroke" d="M16 9c1.5 1.5 1.5 4.5 0 6m3-9c3 3 3 9 0 12" />}</svg>
}

function FullscreenIcon() {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path className="stroke" d="M8 4H4v4m12-4h4v4M8 20H4v-4m12 4h4v-4" /></svg>
}
