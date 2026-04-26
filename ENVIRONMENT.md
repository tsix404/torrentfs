# TorrentFS 开发环境

## 系统依赖

### 必需

| 依赖 | 版本 | 安装 |
|------|------|------|
| Rust | >= 1.95 | rustup |
| libtorrent-rasterbar | 2.0 | `pacman -S libtorrent-rasterbar` (Arch) / `apt install libtorrent-rasterbar-dev` (Debian) |
| FUSE | >= 2.9 | 内核模块 + `fusermount` |

### 验证环境

```bash
# Rust
rustc --version   # 应 >= 1.95.0
cargo --version

# libtorrent
ldconfig -p | grep libtorrent-rasterbar
# 输出: libtorrent-rasterbar.so.2.0 => /usr/lib/libtorrent-rasterbar.so.2.0

# FUSE
ls -la /dev/fuse    # 应存在，权限 crw-rw-rw-
which fusermount    # /usr/bin/fusermount

# 若 /dev/fuse 不存在：
# sudo modprobe fuse
```

### FUSE 用户权限

```bash
# 当前用户需在 fuse 组
groups | grep fuse

# 或将用户加入 fuse 组
sudo usermod -a -G fuse $USER
# 重新登录后生效
```

## 编译

```bash
cd /workspace/self/torrentfs
cargo build
```

## 运行

```bash
# MVP-3+ 挂载
cargo run -- --mount-point /tmp/tfs

# 另一终端操作
ls /tmp/tfs/
cp /workspace/torrentfs/77c8dd8e...torrent /tmp/tfs/metadata/
ls /tmp/tfs/data/

# 卸载
fusermount -u /tmp/tfs
```

## 测试

```bash
# 单元测试
cargo test

# 集成测试（需要 FUSE 环境）
cargo test --test integration

# 手动测试脚本（MVP-7+）
bash scripts/test_fuse.sh /tmp/tfs
```

## 项目路径

| 路径 | 说明 |
|------|------|
| `/workspace/self/torrentfs/` | 项目根目录 |
| `/workspace/torrentfs/` | 测试种子文件（10 个） |
| `~/.local/share/torrentfs/` | 运行时状态目录 |
