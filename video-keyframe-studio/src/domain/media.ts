export type MediaSource =
  | {
      kind: 'local'
      assetId: string
      name: string
      playbackUrl: string
    }
  | {
      kind: 'remote'
      assetId: string
      name: string
      playbackUrl: string
      expiresAt?: string
    }

export interface Keyframe {
  id: string
  timestampMs: number
  thumbnailUrl?: string
}

export interface KeyframeSet {
  assetId: string
  durationMs: number
  windowMs: number
  extractionVersion: string
  frames: Keyframe[]
}

export interface MediaAsset {
  source: MediaSource
  keyframes: KeyframeSet
}

export interface MediaSourceProvider {
  getAsset(): Promise<MediaAsset>
}
