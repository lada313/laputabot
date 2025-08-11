import subprocess
import os
from datetime import datetime

def git_commit_and_push(message="Auto-update JSON files"):
    repo_url = os.getenv("GIT_REPO")
    token = os.getenv("GIT_TOKEN")
    if not repo_url or not token:
        print("❌ Git repo or token not set in environment")
        return

    # Подставляем токен в URL
    if repo_url.startswith("https://"):
        repo_url_with_token = repo_url.replace("https://", f"https://{os.getenv('GIT_USERNAME')}:{token}@")
    else:
        print("❌ Repo URL must start with https://")
        return

    try:
        subprocess.run(["git", "config", "user.email", "bot@render.com"], check=True)
        subprocess.run(["git", "config", "user.name", "Render Bot"], check=True)
        subprocess.run(["git", "add", "*.json"], check=True)
        subprocess.run(["git", "commit", "-m", f"{message} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"], check=True)
        subprocess.run(["git", "push", repo_url_with_token, "HEAD:main"], check=True)
        print("✅ JSON files pushed to GitHub")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")
