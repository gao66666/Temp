import manifest from './mock-media-asset.json'
import type { MediaAsset, MediaSourceProvider } from '../domain/media'

export class MockMediaProvider implements MediaSourceProvider {
  async getAsset(): Promise<MediaAsset> {
    return {
      ...(manifest as MediaAsset),
      source: {
        ...(manifest.source as MediaAsset['source']),
        playbackUrl: `${import.meta.env.BASE_URL}mock/flower.mp4`,
      },
    }
  }
}
