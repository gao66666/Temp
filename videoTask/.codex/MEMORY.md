# 项目记忆索引

本文件只保存轻量索引。进行相关设计或开发前，优先读取对应主题文档。

## 架构与方案

- [视频抽帧任务队列](MEMORY/video-frame-extraction-task-queue.md)
  - 用户上传视频后，由 Redis Streams Consumer Group 分发 FFmpeg 抽帧任务。
  - 使用 PEL、动态租约、失败重试和 DLQ 保证任务可恢复。
  - FFmpeg 完成后创建“任务收尾记录”，由补偿程序分别保证原消息 `XACK` 和 Java 回调最终完成。

