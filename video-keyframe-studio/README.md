# Frameflow 关键帧播放器原型

Electron + React + TypeScript + Vite 的本地视频播放器 MVP。

## 运行

```bash
npm install
npm run dev
```

构建验证：

```bash
npm run build
```

## 当前能力

- 内置 Mock 视频与 0.5 秒时间窗口的关键帧数据
- 在 Renderer 中为 Mock 和用户选择的本地视频生成预览图
- 点击关键帧精确设置 `<video>.currentTime`
- 播放进度与当前关键帧高亮同步
- 上一帧、下一帧、空格播放、全屏与静音控制
- `MediaSourceProvider`、`MediaAsset`、`KeyframeSet` 作为本地/云端统一接口

生产环境应将运行时缩略图生成替换成 Media Utility Process 返回的 FFmpeg 结果；远程实现则由 OpenAPI 自动生成 SDK 获取播放地址和关键帧清单。

## Mock 素材

测试视频来自 MDN `<video>` 交互示例所使用的 CC0 样片：
`https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4`
