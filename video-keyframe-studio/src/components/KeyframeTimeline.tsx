import { useEffect, useRef, type CSSProperties } from 'react'
import type { Keyframe } from '../domain/media'
import { formatTime } from '../utils/time'

interface KeyframeTimelineProps {
  frames: Keyframe[]
  currentFrameId?: string
  selectedFrameId?: string
  thumbnails: Record<string, string>
  isGenerating: boolean
  windowMs: number
  onSeek: (frame: Keyframe) => void
}

export function KeyframeTimeline({
  frames,
  currentFrameId,
  selectedFrameId,
  thumbnails,
  isGenerating,
  windowMs,
  onSeek,
}: KeyframeTimelineProps) {
  const stripRef = useRef<HTMLDivElement>(null)
  const focusedFrameId = selectedFrameId ?? currentFrameId

  useEffect(() => {
    if (!focusedFrameId) return
    stripRef.current
      ?.querySelector<HTMLElement>(`[data-frame-id="${focusedFrameId}"]`)
      ?.scrollIntoView({ behavior: 'smooth', inline: 'nearest', block: 'nearest' })
  }, [focusedFrameId])

  const scroll = (direction: -1 | 1) => {
    stripRef.current?.scrollBy({ left: direction * 560, behavior: 'smooth' })
  }

  return (
    <section className="timeline-panel" aria-label="关键帧时间线">
      <div className="timeline-heading">
        <div>
          <div className="eyebrow">FRAME INDEX</div>
          <h2>关键帧</h2>
        </div>
        <div className="timeline-meta">
          {isGenerating && <span className="generating"><i />正在生成预览</span>}
          <span>{frames.length} 帧</span>
          <span>每 {windowMs / 1000} 秒</span>
        </div>
      </div>

      <div className="strip-shell">
        <button className="scroll-button previous" onClick={() => scroll(-1)} aria-label="向前滚动">
          <Chevron direction="left" />
        </button>
        <div className="keyframe-strip" ref={stripRef}>
          {frames.map((frame, index) => {
            const isCurrent = frame.id === currentFrameId
            const isSelected = frame.id === selectedFrameId
            const cardStyle = {
              '--card-depth-angle': `${-74 + (index % 3) * 2}deg`,
              zIndex: isSelected ? frames.length + 2 : index + 1,
            } as CSSProperties
            return (
              <button
                className={`keyframe-card${isCurrent && !isSelected ? ' playback-active' : ''}${isSelected ? ' selected' : ''}`}
                data-frame-id={frame.id}
                key={frame.id}
                style={cardStyle}
                onClick={() => onSeek(frame)}
                aria-label={`跳转到 ${formatTime(frame.timestampMs, true)}`}
                aria-current={isCurrent ? 'true' : undefined}
              >
                <div className="frame-image">
                  {thumbnails[frame.id] ? (
                    <img src={thumbnails[frame.id]} alt="" draggable={false} />
                  ) : (
                    <div className="thumbnail-skeleton" />
                  )}
                  <span className="frame-number">{String(index + 1).padStart(2, '0')}</span>
                  {isCurrent && <span className="playing-marker"><i />{isSelected ? '已选' : '当前'}</span>}
                </div>
                <span className="frame-time">{formatTime(frame.timestampMs, true)}</span>
              </button>
            )
          })}
        </div>
        <button className="scroll-button next" onClick={() => scroll(1)} aria-label="向后滚动">
          <Chevron direction="right" />
        </button>
      </div>
    </section>
  )
}

function Chevron({ direction }: { direction: 'left' | 'right' }) {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d={direction === 'left' ? 'm15 18-6-6 6-6' : 'm9 18 6-6-6-6'} />
    </svg>
  )
}
