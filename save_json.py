"""Optional synchronization of the public candidates catalog only.

Credentials belong in an OS credential helper or SSH agent, never in URLs.
User portfolios, history, watchlists and trades must never be pushed.
"""
import asyncio
import logging
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
PUBLIC_FILES = frozenset({'candidates.json'})
_GIT_QUEUE = None
_GIT_WORKER_TASK = None


def git_enabled():
    return os.getenv('GIT_SYNC', '0') == '1'


def safe_repo_url():
    # The destination is fixed, so configuration cannot exfiltrate credentials.
    url = os.getenv('GIT_REPO', '')
    if os.getenv('GIT_TOKEN'):
        raise ValueError('Remove GIT_TOKEN; configure a credential helper or SSH agent')
    if url == 'git@github.com:lada313/laputabot.git':
        return url
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.netloc != 'github.com'
            or parsed.path not in ('/lada313/laputabot', '/lada313/laputabot.git')
            or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('GIT_REPO must name lada313/laputabot on github.com without credentials')
    return url


async def run_git(*args):
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0')
    # A legacy token must not be inherited by git or a credential helper.
    env.pop('GIT_TOKEN', None)
    process = await asyncio.create_subprocess_exec(
        'git', *args, cwd=ROOT, env=env,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    try:
        return await asyncio.wait_for(process.wait(), timeout=30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.wait()
        raise


async def _do_push(message):
    if not git_enabled():
        return
    repo = safe_repo_url()
    # Exact paths both when staging and committing: pre-staged private files
    # cannot accidentally be included. Only publish an explicitly requested file.
    match = re.fullmatch(r'Update ([a-z_]+\.json)', message)
    if not match or match[1] not in PUBLIC_FILES:
        return
    path = match[1]
    if await run_git('diff', '--quiet', 'HEAD', '--', path) == 0:
        return
    if await run_git('add', '--', path):
        raise RuntimeError('Git catalog staging failed')
    if await run_git('-c', 'user.email=bot@localhost', '-c', 'user.name=Catalog Bot',
                     'commit', '--only', '-m', 'Update public candidates catalog', '--', path):
        raise RuntimeError('Git catalog commit failed')
    if await run_git('push', repo, 'HEAD:main'):
        raise RuntimeError('Git catalog push failed')


async def _git_worker():
    while True:
        message = await _GIT_QUEUE.get()
        try:
            await _do_push(message)
        except Exception:
            logger.error('Catalog synchronization failed; check credential configuration and connectivity')
        finally:
            _GIT_QUEUE.task_done()


def start_git_worker():
    global _GIT_QUEUE, _GIT_WORKER_TASK
    if git_enabled() and _GIT_QUEUE is None:
        safe_repo_url()
        _GIT_QUEUE = asyncio.Queue(maxsize=1)
        _GIT_WORKER_TASK = asyncio.create_task(_git_worker())


def enqueue_git_push(message):
    if message != 'Update candidates.json' or not git_enabled() or _GIT_QUEUE is None:
        return
    try:
        _GIT_QUEUE.put_nowait(message)
    except asyncio.QueueFull:
        pass  # Coalesce pending catalog updates.
