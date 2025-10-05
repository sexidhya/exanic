# autodeploy.py
from flask import Flask, request
import os, hmac, hashlib, subprocess, threading

SECRET = b"mydeploytoken"  # must match webhook secret (if used)
REPO_DIR = "C:\exanicbot"
SESSION_NAME = "bot7"  # your tmux session name

app = Flask(__name__)

def verify_signature(payload, signature):
    if not signature:
        return False
    sha_name, sig = signature.split("=")
    mac = hmac.new(SECRET, msg=payload, digestmod=hashlib.sha1)
    return hmac.compare_digest(mac.hexdigest(), sig)

@app.route("/github-webhook", methods=["POST"])
def webhook():
    sig = request.headers.get("X-Hub-Signature")
    payload = request.data

    # Verify signature if using secret
    if SECRET and not verify_signature(payload, sig):
        return "Invalid signature", 403

    # Run deploy in a background thread so webhook returns instantly
    threading.Thread(target=deploy).start()
    return "Deploying", 200

def deploy():
    try:
        os.chdir(REPO_DIR)
        print("[AUTO-DEPLOY] Pulling latest changes...")
        subprocess.run(["git", "pull", "origin", "main"], check=True)

        print("[AUTO-DEPLOY] Restarting bot session...")
        # Kill tmux session
        subprocess.run(["tmux", "kill-session", "-t", SESSION_NAME])
        # Start new one detached
        subprocess.run(["tmux", "new-session", "-d", "-s", SESSION_NAME, "python3 bot.py"])
        print("[AUTO-DEPLOY] Done ✅")
    except Exception as e:
        print("[AUTO-DEPLOY] Error:", e)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)
