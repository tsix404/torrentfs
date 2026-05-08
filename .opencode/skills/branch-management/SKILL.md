---
name: branch-management
description: 定义 Multica 六角色团队的分支命名、本地与远程映射、Git 操作规范，以及各角色在各阶段的分支职责。替代 dev-team-collab 中的协作调度（已由 coordinator 接管），仅保留分支管理。开发、审查、测试或合并代码时使用。
---

# 分支管理规范

Coordinator 接管了 issue assign 和阶段流转。本 skill 仅定义分支相关操作。

## 分支架构（Platform 管理，Agent 仅需了解）

```
Bare repo:  .repos/<ws>/<repo>.git
Worktree:   /workspace/multica/<ws>/<run_id>/workdir/
Local:      agent/<role>/<run_id>   (platform 自动创建)
Remote:     origin/multica/$IDENTIFIER   (规范引用名)
```

**Agent 不需要 `git checkout -b`**——platform 已创建 worktree 并配置 tracking。
Agent 只需要知道自己在哪个分支上工作。

## 启动脚本（所有 Agent 通用）

```bash
set -euo pipefail

ISSUE_ID="${ISSUE_ID:-}"
if [ -z "$ISSUE_ID" ]; then
    echo "FATAL: ISSUE_ID not set." >&2
    exit 1
fi

IDENTIFIER=$(multica issue get "$ISSUE_ID" --output json | jq -r '.identifier')
REMOTE_BRANCH="multica/$IDENTIFIER"
LOCAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "ISSUE=$ISSUE_ID  ID=$IDENTIFIER  LOCAL=$LOCAL_BRANCH  REMOTE=$REMOTE_BRANCH"
```

## Git 操作规范

### 开发者（Vexel / Vulcan）

```bash
# 推送代码到远程规范分支
git push -u origin "$LOCAL_BRANCH:$REMOTE_BRANCH"

# 拉取最新
git pull origin "$REMOTE_BRANCH"

# 完成通知（coordinator 会 assign 给 Radian）
multica issue comment add "$ISSUE_ID" --content "Development complete. Branch: $REMOTE_BRANCH (local: $LOCAL_BRANCH)."
```

### 审查者（Radian）

```bash
# 确保在正确分支上
git fetch origin "$REMOTE_BRANCH"

# 查看变更
git log origin/"$REMOTE_BRANCH" --oneline -10
git diff origin/main...origin/"$REMOTE_BRANCH"

# 审查完成后通知（coordinator 会 assign 给 Verity）
multica issue comment add "$ISSUE_ID" --content "**APPROVED**. Branch: $REMOTE_BRANCH reviewed."
```

### 测试者（Verity）

```bash
# 切换到测试分支
git fetch origin "$REMOTE_BRANCH"
git checkout -B "$LOCAL_BRANCH" origin/"$REMOTE_BRANCH"

# 执行测试...

# 完成后通知（coordinator 会 assign 给 Lynx）
multica issue comment add "$ISSUE_ID" --content "**QA_PASSED**. Tested branch: $REMOTE_BRANCH."
```

### PM（Lynx）

```bash
# 合并到 main
git fetch origin
git checkout main
git merge --no-ff origin/"$REMOTE_BRANCH"
git push origin main

# 关闭 issue
multica issue update "$ISSUE_ID" --status done
multica issue comment add "$ISSUE_ID" --content "Merged $REMOTE_BRANCH into main."
```

## 常见问题

### Q: 我该用 LOCAL_BRANCH 还是 REMOTE_BRANCH？
- **评论/通知**：用 `$REMOTE_BRANCH`（规范引用名，跨 agent 可见）
- **git 操作**：用 `$LOCAL_BRANCH`（本地 worktree 分支名）
- **推送**：`git push origin $LOCAL_BRANCH:$REMOTE_BRANCH`（本地→远程规范名）

### Q: platform 创建了 worktree，我还需要 `git checkout` 吗？
- 正常情况下不需要。但如果需要切换分支（如 Verity 需要测试特定分支），使用 `git checkout -B $LOCAL_BRANCH origin/$REMOTE_BRANCH`

### Q: `git push` 直接推可以吗？
- 可以。因为 worktree 的本地分支已 track 到 `origin/$REMOTE_BRANCH`，直接 `git push` 即可。但显式指定 `$LOCAL_BRANCH:$REMOTE_BRANCH` 更安全。