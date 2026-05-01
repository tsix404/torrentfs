# TorrentFS MVP 设计文档

## 项目简介

TorrentFS 是一个基于 FUSE 用户态文件系统的 BitTorrent 虚拟文件系统。

用户通过标准文件操作使用 BitTorrent：
- 将 `.torrent` 文件复制到 `metadata/` 目录
- 自动在 `data/` 下生成对应的种子目录和文件树
- 读取文件时按需触发分块下载（lazy download）
- 已下载的分块自动缓存并做种

## 架构

```
┌──────────────────────────────────────────┐
│              FUSE 层 (torrentfs-fuse)     │
│  metadata/ (写入 .torrent)               │
│  data/<torrent>/<files> (读取触发下载)    │
│         │  MPSC + oneshot               │
│         ▼                                │
│           Core 层 (torrentfs)             │
│  Tokio Runtime · SQLite DB · PieceCache  │
│         │                                │
│         ▼                                │
│     libtorrent 层 (torrentfs-libtorrent)  │
│  Session 管理 · Torrent Handle · Alerts   │
└──────────────────────────────────────────┘
```

**三个 crate：**
| Crate | 职责 |
|-------|------|
| `torrentfs` | 核心库：Tokio runtime、SQLite 持久化、分块缓存、MPSC 协议 |
| `torrentfs-fuse` | FUSE 文件系统实现 + CLI 入口（clap） |
| `torrentfs-libtorrent` | libtorrent-rasterbar FFI 封装 |

## 状态目录

```
~/.local/share/torrentfs/
├── db/
│   └── metadata.db          # SQLite（torrents + torrent_files 表）
├── cache/
│   └── pieces/<info_hash>/  # 分块缓存（按 info_hash 组织）
└── state/
    └── resume/              # resume_data 快照
```

## 数据库 Schema

### torrents 表

```sql
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash BLOB NOT NULL UNIQUE,        -- 种子 info_hash（20字节 SHA1）
    name TEXT NOT NULL,                     -- 种子名称
    total_size INTEGER NOT NULL,            -- 总大小（字节）
    file_count INTEGER NOT NULL,            -- 文件数量
    status TEXT NOT NULL DEFAULT 'pending', -- 状态：pending/downloading/seeding/error
    source_path TEXT NOT NULL DEFAULT '',   -- 原始 .torrent 文件路径
    torrent_data BLOB,                      -- 原始 .torrent 文件内容（支持重启恢复）
    resume_data BLOB,                       -- libtorrent resume data（支持重启恢复）
    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### torrent_files 表

```sql
CREATE TABLE torrent_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    torrent_id INTEGER NOT NULL,            -- 关联 torrents.id
    path TEXT NOT NULL,                     -- 文件相对路径
    size INTEGER NOT NULL,                  -- 文件大小（字节）
    first_piece INTEGER NOT NULL DEFAULT 0, -- 文件起始 piece 索引
    last_piece INTEGER NOT NULL DEFAULT 0,  -- 文件结束 piece 索引
    FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,
    UNIQUE(torrent_id, path)
);
```

### 索引

- `idx_torrents_info_hash` — 按 info_hash 快速查找
- `idx_torrents_status` — 按状态筛选种子
- `idx_torrents_source_path` — 按源路径查找
- `idx_torrent_files_torrent_id` — 按种子 ID 查找文件
- `idx_torrent_files_path` — 按路径查找文件

## MVP 阶段总览

| 阶段 | 目标 | 可演示 |
|------|------|--------|
| **MVP-1** | Torrent Info CLI | `cargo run -- file.torrent` → 打印元数据 |
| **MVP-2** | SQLite 持久化 | 解析 → DB 写入 → 重启可查 |
| **MVP-3** | FUSE 空挂载 | mount → `metadata/` 接受 .torrent 写入 |
| **MVP-4** | 生命周期闭环 | 写入 `metadata/` → 自动解析 → `data/` 可浏览 |
| **MVP-5** | 按需读取 | `cat data/<torrent>/<file>` → 触发下载 → 缓存 |
| **MVP-6** | 生产就绪 | 做种、恢复、优雅关闭、死锁预防 |

## MVP 成功标准

- ✅ 稳定挂载（无 crash、无泄漏）
- ✅ 添加 torrent 后立即可浏览文件结构
- ✅ 文件读取触发分块下载
- ✅ 重启后状态恢复（种子列表 + 缓存）
- ✅ 无 FUSE 死锁（并发读通过）

## 技术决策

| 决策 | 选择 | 原因 |
|------|------|------|
| 数据库 | SQLite (sqlx) | 嵌入式、无需服务端、单文件 |
| 异步运行时 | Tokio (current_thread) | 轻量、与 sqlx 集成好 |
| libtorrent 绑定 | libtorrent-sys (FFI) | 系统已装 libtorrent-rasterbar 2.0 |
| FUSE 库 | fuser 0.15 | 纯 Rust、不依赖 libfuse C 库 |
| FUSE 线程模型 | spawn_mount（多线程） | 避免阻塞 FUSE 回调，支持 tokio mpsc |
| 缓存策略 | 全量缓存（MVP 阶段无淘汰） | 简化实现 |

## 测试种子

位置：`/workspace/torrentfs/`
- 10 个真实 BitTorrent 文件（2.3KB ~ 426KB）
- 包含小文件种子和 Ubuntu ISO torrent
