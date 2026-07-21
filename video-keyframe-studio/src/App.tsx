import { useEffect, useRef, useState } from 'react'
import { VideoWorkspace } from './components/VideoWorkspace'
import { MockMediaProvider } from './data/mockMediaProvider'
import type { MediaAsset } from './domain/media'

const mockProvider = new MockMediaProvider()

function App() {
  const [asset, setAsset] = useState<MediaAsset>()
  const [isLoading, setIsLoading] = useState(true)
  const inputRef = useRef<HTMLInputElement>(null)
  const objectUrlRef = useRef<string | undefined>(undefined)

  useEffect(() => {
    void mockProvider.getAsset().then((mockAsset) => {
      setAsset(mockAsset)
      setIsLoading(false)
    })

    return () => {
      if (objectUrlRef.current) URL.revokeObjectURL(objectUrlRef.current)
    }
  }, [])

  const openLocalVideo = (file?: File) => {
    if (!file) return
    if (objectUrlRef.current) URL.revokeObjectURL(objectUrlRef.current)
    const playbackUrl = URL.createObjectURL(file)
    objectUrlRef.current = playbackUrl

    setAsset({
      source: {
        kind: 'local',
        assetId: `local-${file.name}-${file.lastModified}`,
        name: file.name,
        playbackUrl,
      },
      keyframes: {
        assetId: `local-${file.name}-${file.lastModified}`,
        durationMs: 0,
        windowMs: 2500,
        extractionVersion: 'runtime-window-v1',
        frames: [],
      },
    })
  }

  if (isLoading || !asset) {
    return <div className="app-loading"><span />正在准备工作台</div>
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><span /></div>
          <span>Frameflow</span>
        </div>
        <div className="asset-title">
          <span className="asset-label">当前素材</span>
          <strong>{asset.source.name}</strong>
        </div>
        <div className="topbar-actions">
          <span className="saved-state"><i />Mock 数据已就绪</span>
          <input
            ref={inputRef}
            className="visually-hidden"
            type="file"
            accept="video/mp4,video/webm,video/quicktime,video/*"
            onChange={(event) => openLocalVideo(event.currentTarget.files?.[0])}
          />
          <button className="open-button" onClick={() => inputRef.current?.click()}>
            <UploadIcon />打开本地视频
          </button>
        </div>
      </header>

      <VideoWorkspace key={asset.source.assetId} asset={asset} />
      <footer className="hint-bar">
        <span><kbd>←</kbd><kbd>→</kbd> 切换关键帧</span>
        <span><kbd>Space</kbd> 播放 / 暂停</span>
        <span>双击画面进入全屏</span>
      </footer>
    </div>
  )
}

function UploadIcon() {
  return <svg viewBox="0 0 24 24" aria-hidden="true"><path className="stroke" d="M12 16V4m0 0L7.5 8.5M12 4l4.5 4.5M5 14v5h14v-5" /></svg>
}

export default App
