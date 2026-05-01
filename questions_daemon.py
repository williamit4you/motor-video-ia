import os
import json
import time
import requests
from datetime import datetime


NEXT_BASE_URL = os.environ.get("NEXT_JS_BASE_URL", "http://localhost:3000").rstrip("/")
SECRET = os.environ.get("WORKER_SECRET_KEY", "super-secret-worker-key-123")


def now_ptbr():
  return datetime.now().strftime("%H:%M:%S")


def log(msg: str):
  print(f"[questions_daemon {now_ptbr()}] {msg}", flush=True)


def get_config():
  url = f"{NEXT_BASE_URL}/api/video-questions/config"
  res = requests.get(url, headers={"x-worker-secret": SECRET}, timeout=15)
  res.raise_for_status()
  return res.json()


def claim_next():
  url = f"{NEXT_BASE_URL}/api/video-questions/next"
  res = requests.get(url, headers={"x-worker-secret": SECRET}, timeout=20)
  res.raise_for_status()
  return res.json()


def patch_question(qid: str, data: dict):
  url = f"{NEXT_BASE_URL}/api/video-questions/{qid}"
  res = requests.patch(url, headers={"Content-Type": "application/json"}, data=json.dumps(data), timeout=30)
  res.raise_for_status()
  return res.json()


def create_project(question_text: str, cfg: dict, use_external_media: bool):
  url = f"{NEXT_BASE_URL}/api/video-code/projects"
  payload = {
    "ideaPrompt": question_text,
    "aspectRatio": cfg.get("defaultAspectRatio", "PORTRAIT_9_16"),
    "videoDurationSec": cfg.get("videoDurationSec", 30),
    "ttsVoice": cfg.get("ttsVoice", "pt-BR-AntonioNeural"),
    "ttsSpeed": cfg.get("ttsSpeed", "+5%"),
    "useExternalMedia": use_external_media,
  }
  res = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload), timeout=30)
  res.raise_for_status()
  return res.json()


def generate_with_ai(project_id: str):
  url = f"{NEXT_BASE_URL}/api/video-code/generate"
  res = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps({"projectId": project_id}), timeout=120)
  res.raise_for_status()
  return res.json()


def render_mp4(project_id: str):
  url = f"{NEXT_BASE_URL}/api/video-code/render"
  res = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps({"projectId": project_id}), timeout=3600)
  res.raise_for_status()
  return res.json()


def enqueue_social(qid: str, platform: str, post_type: str = "REEL"):
  url = f"{NEXT_BASE_URL}/api/video-questions/{qid}/enqueue-social"
  res = requests.post(
    url,
    headers={"Content-Type": "application/json", "x-worker-secret": SECRET},
    data=json.dumps({"platform": platform, "postType": post_type}),
    timeout=30,
  )
  if res.status_code >= 400:
    log(f"⚠️ enqueue {platform} falhou: HTTP {res.status_code} {res.text[:160]}")
    return None
  return res.json()


def process_one(cfg: dict):
  claimed = claim_next()
  q = claimed.get("question")
  if not q:
    return False

  qid = q["id"]
  text = q["questionText"]
  # Use flag from question if true, else fallback to global config
  use_external_media = q.get("useExternalMedia", False) or cfg.get("useExternalMedia", False)
  
  log(f"🧠 Processando pergunta {qid}: {text[:70]}... (ExternalMedia: {use_external_media})")

  try:
    project = create_project(text, cfg, use_external_media)
    patch_question(qid, {"codeVideoProjectId": project["id"]})

    log("🤖 Gerando roteiro/cenas...")
    generate_with_ai(project["id"])

    log("🎬 Renderizando MP4 (Remotion + áudio via worker)...")
    rendered = render_mp4(project["id"])

    patch_question(qid, {"status": "DONE", "completedAt": datetime.utcnow().isoformat()})
    log(f"✅ Concluído. MP4: {rendered.get('videoUrl')}")

    if cfg.get("autoEnqueueMetaReels"):
      enqueue_social(qid, "META", "REEL")
    if cfg.get("autoEnqueueMetaStory"):
      enqueue_social(qid, "META", "STORY")
    if cfg.get("autoEnqueueTikTok"):
      enqueue_social(qid, "TIKTOK", "REEL")
    if cfg.get("autoEnqueueLinkedIn"):
      enqueue_social(qid, "LINKEDIN", "REEL")
    if cfg.get("autoEnqueueYouTube"):
      enqueue_social(qid, "YOUTUBE", "REEL")

    return True
  except Exception as e:
    msg = str(e)
    patch_question(qid, {"status": "FAILED", "errorMessage": msg})
    log(f"❌ Falhou: {msg}")
    return True


def main():
  log(f"NEXT_JS_BASE_URL={NEXT_BASE_URL}")
  while True:
    try:
      cfg = get_config()
      if not cfg.get("isEnabled"):
        time.sleep(10)
        continue

      did = False
      max_per = int(cfg.get("maxQuestionsPerRun") or 1)
      for _ in range(max_per):
        did = process_one(cfg) or did
        if not did:
          break

      # Interval
      interval_minutes = int(cfg.get("intervalMinutes") or 60)
      sleep_s = max(10, interval_minutes * 60)
      time.sleep(10 if did else sleep_s)
    except Exception as e:
      log(f"⚠️ loop error: {e}")
      time.sleep(10)


if __name__ == "__main__":
  main()
