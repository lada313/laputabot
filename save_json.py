# save_json.py
import os, asyncio
from datetime import datetime

# Очередь задач на пуш
_GIT_QUEUE: asyncio.Queue | None = None
_GIT_WORKER_TASK: asyncio.Task | None = None

def git_enabled() -> bool:
    # Удобный флажок: GIT_SYNC=0 — полностью выключить пуши в проде
    return os.getenv("GIT_SYNC", "1") == "1"

async def _do_push(message: str):
    """Неблокирующий git push через create_subprocess_exec."""
    if not git_enabled():
        return
    repo_url = os.getenv("GIT_REPO")
    token = os.getenv("GIT_TOKEN")
    user = os.getenv("GIT_USERNAME", "render-bot")
    if not (repo_url and token and repo_url.startswith("https://")):
        print("❌ Git not configured, skip push")
        return
    repo_url_with_token = repo_url.replace("https://", f"https://{user}:{token}@")
    msg = f"{message} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    # Последовательно: config/add/commit/push
    import sys
    async def run(*args):
        p = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await p.communicate()
        if p.returncode != 0:
            print("❌ git:", args, (out or b"").decode()[:200], (err or b"").decode()[:200], file=sys.stderr)
            raise RuntimeError(f"git failed: {args}")
    try:
        await run("git", "config", "user.email", "bot@render.com")
        await run("git", "config", "user.name", "Render Bot")
        await run("git", "add", "*.json")
        await run("git", "commit", "-m", msg)
    except Exception:
        # Нет изменений — это нормально
        return
    await run("git", "push", repo_url_with_token, "HEAD:main")
    print("✅ JSON files pushed")

async def _git_worker():
    """Один-единственный воркер, который по очереди делает пуши."""
    assert _GIT_QUEUE is not None
    while True:
        message = await _GIT_QUEUE.get()
        try:
            await _do_push(message)
        except Exception as e:
            print("❌ push error:", e)
        finally:
            _GIT_QUEUE.task_done()

def start_git_worker():
    """Вызываем один раз при старте приложения."""
    global _GIT_QUEUE, _GIT_WORKER_TASK
    if _GIT_QUEUE is None:
        _GIT_QUEUE = asyncio.Queue()
        _GIT_WORKER_TASK = asyncio.create_task(_git_worker())

def enqueue_git_push(message: str):
    """Кладём задачу в очередь (мгновенно, не блокирует обработчик)."""
    if not git_enabled():
        return
    if _GIT_QUEUE is None:
        # Если забыли вызвать start_git_worker — просто тихо не пушим
        return
    _GIT_QUEUE.put_nowait(message)
