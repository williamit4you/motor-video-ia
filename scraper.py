import os
import time
import requests
import json
import traceback
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from datetime import datetime, timedelta
from urllib.parse import urljoin
import boto3

# ─── CARREGA .ENV ────────────────────────────────────────────────────────────
from dotenv import load_dotenv
# Tenta .env local primeiro, depois diretório pai (projeto Next.js)
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# ─── CONFIGURAÇÕES DE URL ─────────────────────────────────────────────────────
# ATENÇÃO: Em produção (Easypanel), estas URLs precisam apontar para o
# hostname interno do serviço Next.js, NÃO localhost.
# Exemplo: NEXT_JS_BASE_URL=http://landpage:3000
NEXT_JS_BASE_URL   = os.environ.get("NEXT_JS_BASE_URL", "http://localhost:3000")
NEXT_JS_INGEST_URL = os.environ.get("NEXT_JS_INGEST_URL",  f"{NEXT_JS_BASE_URL}/api/worker/ingest")
NEXT_JS_SOURCES_URL= os.environ.get("NEXT_JS_SOURCES_URL", f"{NEXT_JS_BASE_URL}/api/worker/sources")
NEXT_JS_LOG_URL    = os.environ.get("NEXT_JS_LOG_URL",    f"{NEXT_JS_BASE_URL}/api/pipeline/log")
SECRET_CRON_KEY    = os.environ.get("WORKER_SECRET_KEY", "super-secret-worker-key-123")
FASTAPI_URL        = os.environ.get("FASTAPI_URL", "http://localhost:8000/gerar-video")
PEXELS_API_KEY     = os.environ.get("PEXELS_API_KEY", "")

# ─── CONFIGURAÇÕES MINIO / S3 ─────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME= os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

INTERVAL_HOURS = 6

# ─── INIT S3 ──────────────────────────────────────────────────────────────────
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )
except Exception as e:
    print(f"[INIT] S3 Client init error: {e}")
    s3_client = None

# ─── LOGGING PARA O FRONTEND ──────────────────────────────────────────────────

def log_pipeline(step: str, message: str, level: str = "INFO"):
    """Envia um log de status para o Next.js exibir em tempo real na UI."""
    print(f"[{level}] [{step}] {message}")
    try:
        res = requests.post(
            NEXT_JS_LOG_URL,
            json={"step": step, "message": message, "level": level},
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=5
        )
        if res.status_code != 200:
            print(f"[LOG_PIPELINE] Resposta inesperada {res.status_code}: {res.text[:80]}")
    except Exception as e:
        print(f"[LOG_PIPELINE] Falha ao enviar log: {e}")

# ─── DIAGNÓSTICO DE AMBIENTE ──────────────────────────────────────────────────

def print_env_diagnostics():
    """Imprime e loga todas as configurações críticas ao iniciar."""
    lines = [
        "═══════════ DIAGNÓSTICO DE AMBIENTE ═══════════",
        f"NEXT_JS_BASE_URL    = {NEXT_JS_BASE_URL}",
        f"NEXT_JS_INGEST_URL  = {NEXT_JS_INGEST_URL}",
        f"NEXT_JS_SOURCES_URL = {NEXT_JS_SOURCES_URL}",
        f"NEXT_JS_LOG_URL     = {NEXT_JS_LOG_URL}",
        f"FASTAPI_URL         = {FASTAPI_URL}",
        f"PEXELS_API_KEY      = {'✅ configurada' if PEXELS_API_KEY else '❌ NÃO configurada'}",
        f"MINIO_ENDPOINT      = {MINIO_ENDPOINT or '❌ NÃO configurado'}",
        f"MINIO_PUBLIC_URL    = {MINIO_PUBLIC_URL or '❌ NÃO configurado'}",
        f"OPENAI_API_KEY      = {'✅ configurada' if os.environ.get('OPENAI_API_KEY') else '❌ NÃO configurada'}",
        f"S3 Client           = {'✅ ok' if s3_client else '❌ falhou na inicialização'}",
        "═══════════════════════════════════════════════",
    ]
    for line in lines:
        print(line)

def test_connectivity():
    """Testa conectividade com todos os serviços críticos."""
    log_pipeline("INIT", "🔌 Testando conectividade com os serviços...")

    # Testa Next.js
    try:
        res = requests.get(NEXT_JS_SOURCES_URL, timeout=5)
        log_pipeline("INIT", f"✅ Next.js acessível ({NEXT_JS_SOURCES_URL}) → HTTP {res.status_code}", "SUCCESS")
    except Exception as e:
        log_pipeline("INIT", f"❌ ERRO: Não consegui acessar o Next.js em {NEXT_JS_SOURCES_URL} → {e}\n"
                              f"   ⚠️ Se estiver no Easypanel, configure NEXT_JS_BASE_URL com o hostname interno do serviço.", "ERROR")

    # Testa FastAPI
    try:
        res = requests.get(FASTAPI_URL.replace("/gerar-video", "/docs"), timeout=5)
        log_pipeline("INIT", f"✅ FastAPI/video acessível → HTTP {res.status_code}", "SUCCESS")
    except Exception as e:
        log_pipeline("INIT", f"❌ ERRO: FastAPI não acessível em {FASTAPI_URL} → {e}", "ERROR")

    # Testa Pexels
    if PEXELS_API_KEY:
        try:
            res = requests.get("https://api.pexels.com/v1/search",
                               headers={"Authorization": PEXELS_API_KEY},
                               params={"query": "test", "per_page": 1}, timeout=8)
            log_pipeline("INIT", f"✅ Pexels API acessível → HTTP {res.status_code}", "SUCCESS")
        except Exception as e:
            log_pipeline("INIT", f"❌ Pexels API erro: {e}", "ERROR")
    else:
        log_pipeline("INIT", "⚠️ PEXELS_API_KEY não configurada — vídeos terão fundo preto", "WARN")

    # Testa MinIO/S3
    if s3_client and MINIO_BUCKET_NAME:
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET_NAME)
            log_pipeline("INIT", f"✅ MinIO acessível — bucket '{MINIO_BUCKET_NAME}' ok", "SUCCESS")
        except Exception as e:
            log_pipeline("INIT", f"❌ MinIO erro: {e}", "ERROR")
    else:
        log_pipeline("INIT", "❌ MinIO não configurado — vídeos não serão salvos", "ERROR")

# ─── CONFIG DINÂMICA DO ADMIN ─────────────────────────────────────────────────

def get_dynamic_config():
    """Lê do Next.js quais scrapers estão ativos e se o botão manual foi acionado."""
    try:
        res = requests.get(NEXT_JS_SOURCES_URL, timeout=8)
        if res.status_code == 200:
            data = res.json()
            return data
        else:
            log_pipeline("INIT", f"⚠️ get_dynamic_config: HTTP {res.status_code} de {NEXT_JS_SOURCES_URL}", "WARN")
    except Exception as e:
        log_pipeline("ERROR", f"❌ Sem comunicação com Next.js: {e}", "ERROR")
    return {"sources": [], "trigger_now": False}

# ─── SCRAPING ─────────────────────────────────────────────────────────────────

def fetch_and_parse(url: str) -> list:
    """Acessa a página-fonte e extrai links de artigos."""
    log_pipeline("FETCH", f"🌐 Acessando fonte: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        log_pipeline("FETCH", f"📡 HTTP {response.status_code} — {len(response.text)} chars recebidos")

        if response.status_code != 200:
            log_pipeline("FETCH", f"❌ Falha HTTP {response.status_code} ao acessar {url}", "ERROR")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        all_links = soup.select('a')
        log_pipeline("FETCH", f"🔗 {len(all_links)} links totais encontrados na página")

        links = []
        for a in all_links[:50]:
            href = a.get('href', '')
            if href and any(kw in href for kw in ['noticia', 'tecnologia', 'tudo-sobre', 'cnnbrasil', '/tech/', '/ia/', '/inteligencia']):
                if href.startswith('/'):
                    href = urljoin(url, href)
                if href.startswith('http'):
                    links.append(href)

        unique = list(set(links))[:3]
        log_pipeline("FETCH", f"✅ {len(unique)} link(s) válido(s) selecionados para processar")
        return unique

    except requests.Timeout:
        log_pipeline("FETCH", f"❌ Timeout (15s) ao acessar {url}", "ERROR")
        return []
    except Exception as e:
        log_pipeline("FETCH", f"❌ Erro ao acessar {url}: {e}", "ERROR")
        return []

def read_article_text(article_url: str) -> str:
    """Lê o texto completo de um artigo."""
    log_pipeline("FETCH", f"📰 Lendo artigo: {article_url[:80]}")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(article_url, headers=headers, timeout=15)
        log_pipeline("FETCH", f"📡 HTTP {res.status_code} — {len(res.text)} chars do artigo")

        if res.status_code != 200:
            log_pipeline("FETCH", f"❌ HTTP {res.status_code} ao ler artigo", "ERROR")
            return ""

        soup = BeautifulSoup(res.text, "html.parser")

        # Remove scripts/styles
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        paragraphs = soup.find_all('p')
        full_text = " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])

        log_pipeline("FETCH", f"📄 Texto extraído: {len(full_text)} caracteres, {len(paragraphs)} parágrafos")

        if len(full_text) < 300:
            log_pipeline("FETCH", f"⚠️ Texto muito curto ({len(full_text)} chars) — provavelmente paywall ou página vazia", "WARN")

        return full_text

    except requests.Timeout:
        log_pipeline("FETCH", f"❌ Timeout ao ler artigo {article_url[:60]}", "ERROR")
        return ""
    except Exception as e:
        log_pipeline("FETCH", f"❌ Erro ao ler artigo: {e}", "ERROR")
        return ""

# ─── IA ───────────────────────────────────────────────────────────────────────

def rewrite_with_ai(raw_text: str):
    """Usa GPT-4o-mini para reescrever o artigo e gerar o roteiro."""
    log_pipeline("AI", f"🤖 Enviando {len(raw_text[:8000])} chars para GPT-4o-mini...")

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.7)

    prompt = ChatPromptTemplate.from_messages([
        ("system", """Você é um jornalista independente de tecnologia focado em alta conversão SEO.
Seu objetivo é ler um texto raw raspado da internet, e REESCREVÊ-LO por completo com suas palavras,
garantindo que NENHUM plágio seja detectado, mas mantendo 100% da precisão dos fatos noticiados.
Você deve outputar um JSON rigorosamente estruturado com:
- "title": Um título impactante (SEM clickbait exagerado, formato editorial)
- "summary": Um roteiro ENGAJADOR e direto de até 30 segundos de locução para um vídeo TikTok/Reels/Story baseado na notícia (máx 450 caracteres).
- "content_html": O artigo escrito, formatado com tags HTML semânticas como <p>, <h2>, e <b>. Formato pronto pro TipTap Editor.
"""),
        ("user", "Texto Original Bruto: {raw_text}")
    ])

    chain = prompt | llm | JsonOutputParser()
    try:
        log_pipeline("AI", "⏳ Aguardando resposta da OpenAI...")
        resultado = chain.invoke({"raw_text": raw_text[:8000]})

        if not resultado:
            log_pipeline("AI", "❌ IA retornou resultado vazio", "ERROR")
            return None

        title = resultado.get('title', 'sem título')
        summary = resultado.get('summary', '')
        content = resultado.get('content_html', '')

        log_pipeline("AI", f"✅ Título gerado: {title[:70]}", "SUCCESS")
        log_pipeline("AI", f"📝 Resumo (roteiro): {summary[:80]}...")
        log_pipeline("AI", f"📄 Artigo HTML: {len(content)} chars gerados")

        return resultado

    except Exception as e:
        log_pipeline("AI", f"❌ Erro na chamada OpenAI: {e}", "ERROR")
        log_pipeline("AI", f"   Traceback: {traceback.format_exc()[-300:]}", "ERROR")
        return None

# ─── MINIO UPLOAD ─────────────────────────────────────────────────────────────

def upload_to_minio(file_path: str, object_name: str) -> str | None:
    if not s3_client:
        log_pipeline("UPLOAD", "❌ S3 client não inicializado — upload impossível", "ERROR")
        return None

    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    log_pipeline("UPLOAD", f"⬆️ Enviando para MinIO: {object_name} ({file_size // 1024}KB)")

    try:
        s3_client.upload_file(
            file_path, MINIO_BUCKET_NAME, object_name,
            ExtraArgs={'ContentType': 'video/mp4'}
        )
        url = f"{MINIO_PUBLIC_URL}/{object_name}"
        log_pipeline("UPLOAD", f"✅ Upload concluído: {url}", "SUCCESS")
        return url
    except Exception as e:
        log_pipeline("UPLOAD", f"❌ Erro no upload MinIO: {e}", "ERROR")
        return None

# ─── PEXELS ───────────────────────────────────────────────────────────────────

def fetch_pexels_media(query: str) -> list:
    """Busca vídeos no Pexels (fallback para fotos estáticas)."""
    if not PEXELS_API_KEY:
        log_pipeline("PEXELS", "⚠️ PEXELS_API_KEY não configurada — usando fundo preto", "WARN")
        return []

    log_pipeline("PEXELS", f"🎥 Buscando vídeos portrait para: '{query}'")
    headers = {"Authorization": PEXELS_API_KEY}
    local_paths = []

    # Tenta vídeos portrait
    try:
        res = requests.get(
            "https://api.pexels.com/videos/search",
            headers=headers,
            params={"query": query, "per_page": 5, "orientation": "portrait", "size": "medium"},
            timeout=15
        )
        data = res.json()
        total = data.get("total_results", 0)
        videos = data.get("videos", [])[:2]
        log_pipeline("PEXELS", f"📊 Pexels encontrou {total} vídeos para '{query}' — baixando {len(videos)}")

        for video in videos:
            try:
                files = video.get("video_files", [])
                # Prefere portrait >= 720p
                portrait = [f for f in files if f.get("height", 0) >= 720]
                chosen = sorted(portrait or files, key=lambda x: x.get("height", 0))
                if not chosen:
                    continue

                video_url = chosen[0]["link"]
                video_id = video['id']
                temp_path = f"temp_pexels_{int(time.time())}_{video_id}.mp4"

                log_pipeline("PEXELS", f"⬇️ Baixando vídeo ID {video_id} ({chosen[0].get('height', '?')}p)...")
                vres = requests.get(video_url, timeout=120, stream=True)
                downloaded = 0
                with open(temp_path, "wb") as f:
                    for chunk in vres.iter_content(chunk_size=65536):
                        f.write(chunk)
                        downloaded += len(chunk)

                size_kb = downloaded // 1024
                local_paths.append(temp_path)
                log_pipeline("PEXELS", f"✅ Vídeo Pexels ID {video_id} baixado ({size_kb}KB)", "SUCCESS")

            except Exception as e:
                log_pipeline("PEXELS", f"❌ Erro ao baixar vídeo Pexels: {e}", "ERROR")

    except Exception as e:
        log_pipeline("PEXELS", f"❌ Erro na busca de vídeos Pexels: {e}", "ERROR")

    # Fallback: fotos estáticas
    if not local_paths:
        log_pipeline("PEXELS", f"🖼️ Nenhum vídeo encontrado — tentando fotos para: '{query}'")
        try:
            res = requests.get(
                "https://api.pexels.com/v1/search",
                headers=headers,
                params={"query": query, "per_page": 5, "orientation": "portrait"},
                timeout=15
            )
            data = res.json()
            photos = data.get("photos", [])[:3]
            log_pipeline("PEXELS", f"📊 {len(photos)} foto(s) encontradas para '{query}'")

            for photo in photos:
                try:
                    img_url = photo["src"]["portrait"]
                    temp_path = f"temp_pexels_{int(time.time())}_{photo['id']}.jpg"

                    pres = requests.get(img_url, timeout=30)
                    with open(temp_path, "wb") as f:
                        f.write(pres.content)

                    local_paths.append(temp_path)
                    log_pipeline("PEXELS", f"✅ Foto ID {photo['id']} baixada ({len(pres.content)//1024}KB)", "SUCCESS")

                except Exception as e:
                    log_pipeline("PEXELS", f"❌ Erro ao baixar foto Pexels: {e}", "ERROR")

        except Exception as e:
            log_pipeline("PEXELS", f"❌ Erro na busca de fotos Pexels: {e}", "ERROR")

    if not local_paths:
        log_pipeline("PEXELS", "⚠️ Nenhuma mídia Pexels disponível — vídeo terá fundo padrão", "WARN")

    return local_paths

# ─── GERAÇÃO DE VÍDEO ─────────────────────────────────────────────────────────

def generate_video_and_upload(summary_text: str, keywords: str = "") -> str | None:
    log_pipeline("VIDEO", f"🎬 Iniciando geração de vídeo portrait...")
    log_pipeline("VIDEO", f"📝 Roteiro ({len(summary_text)} chars): {summary_text[:80]}...")

    # Busca fundo Pexels
    pexels_paths = []
    if keywords:
        log_pipeline("VIDEO", f"🔑 Keywords para Pexels: '{keywords}'")
        pexels_paths = fetch_pexels_media(keywords)
    else:
        log_pipeline("VIDEO", "⚠️ Sem keywords — usando fundo preto", "WARN")

    try:
        data_fields = {
            "text": summary_text,
            "video_format": "portrait",
            "background_color": "#111827",
            "speed": "+5%"
        }

        multipart_files = [("dummy", ("", ""))]
        open_handles = []

        if pexels_paths:
            log_pipeline("VIDEO", f"🎨 Montando vídeo com {len(pexels_paths)} mídia(s) Pexels como fundo")
            durations = [round(100 / len(pexels_paths), 1)] * len(pexels_paths)
            data_fields["image_durations"] = json.dumps(durations)

            for path in pexels_paths:
                ext = os.path.splitext(path)[1].lower()
                mime = "video/mp4" if ext == ".mp4" else "image/jpeg"
                fh = open(path, "rb")
                open_handles.append(fh)
                multipart_files.append(("images", (os.path.basename(path), fh, mime)))
                log_pipeline("VIDEO", f"📎 Adicionando mídia: {os.path.basename(path)} ({mime})")
        else:
            log_pipeline("VIDEO", "🖤 Sem fundo Pexels — vídeo com cor sólida #111827")

        log_pipeline("VIDEO", f"📤 Enviando para FastAPI: {FASTAPI_URL}")
        log_pipeline("VIDEO", "⏳ Gerando áudio TTS + legendas Whisper + renderizando... (pode levar 2-5 min)")

        start_time = time.time()
        res = requests.post(FASTAPI_URL, data=data_fields, files=multipart_files, timeout=600)
        elapsed = round(time.time() - start_time, 1)

        # Fecha file handles
        for fh in open_handles:
            fh.close()

        # Limpa temporários Pexels
        for path in pexels_paths:
            if os.path.exists(path):
                os.remove(path)

        log_pipeline("VIDEO", f"📡 FastAPI respondeu HTTP {res.status_code} em {elapsed}s")

        if res.status_code == 200:
            video_size_kb = len(res.content) // 1024
            log_pipeline("VIDEO", f"✅ Vídeo recebido da FastAPI: {video_size_kb}KB", "SUCCESS")

            temp_path = f"temp_worker_video_{int(time.time())}.mp4"
            with open(temp_path, "wb") as f:
                f.write(res.content)

            log_pipeline("VIDEO", f"💾 Vídeo salvo temporariamente: {temp_path}")

            object_name = f"stories/story_{int(time.time())}.mp4"
            s3_url = upload_to_minio(temp_path, object_name)

            if os.path.exists(temp_path):
                os.remove(temp_path)

            if s3_url:
                log_pipeline("VIDEO", f"🔗 URL final do vídeo: {s3_url}", "SUCCESS")
            else:
                log_pipeline("VIDEO", "❌ Upload MinIO falhou — vídeo não ficará acessível", "ERROR")

            return s3_url

        else:
            error_body = res.text[:200]
            log_pipeline("VIDEO", f"❌ FastAPI retornou erro {res.status_code}: {error_body}", "ERROR")
            return None

    except requests.Timeout:
        log_pipeline("VIDEO", f"❌ Timeout (600s) — FastAPI demorou mais de 10min para gerar o vídeo", "ERROR")
        # Limpa temporários em caso de falha
        for fh in open_handles:
            try: fh.close()
            except: pass
        for path in pexels_paths:
            if os.path.exists(path): os.remove(path)
        return None

    except Exception as e:
        log_pipeline("VIDEO", f"❌ Erro inesperado na geração de vídeo: {e}", "ERROR")
        log_pipeline("VIDEO", f"   {traceback.format_exc()[-400:]}", "ERROR")
        return None

# ─── INGEST NO NEXT.JS ────────────────────────────────────────────────────────

def push_to_nextjs(article_data: dict, source_url: str, video_url: str | None):
    """Envia o artigo processado para o banco de dados via Next.js."""
    title = article_data.get("title", "Sem título")
    summary = article_data.get("summary", "")
    content = article_data.get("content_html", "")

    log_pipeline("INGEST", f"💾 Salvando no banco: '{title[:60]}'")
    log_pipeline("INGEST", f"   Resumo: {len(summary)} chars | Conteúdo: {len(content)} chars | Vídeo: {'✅' if video_url else '❌ sem vídeo'}")

    payload = {
        "title": title,
        "summary": summary,
        "content": content,
        "sourceUrl": source_url,
        "videoUrl": video_url,
        "secret": SECRET_CRON_KEY
    }

    try:
        res = requests.post(NEXT_JS_INGEST_URL, json=payload, timeout=20)
        log_pipeline("INGEST", f"📡 Next.js respondeu HTTP {res.status_code}")

        if res.status_code == 200:
            log_pipeline("INGEST", f"✅ Notícia salva com sucesso: '{title[:60]}'", "SUCCESS")
            return True
        elif res.status_code == 409:
            log_pipeline("INGEST", f"⚠️ Notícia duplicada (já existe): {source_url[:60]}", "WARN")
        else:
            log_pipeline("INGEST", f"❌ Erro ao salvar: HTTP {res.status_code} — {res.text[:100]}", "ERROR")

    except requests.Timeout:
        log_pipeline("INGEST", f"❌ Timeout ao chamar Next.js ingest", "ERROR")
    except Exception as e:
        log_pipeline("INGEST", f"❌ Falha de conexão com Next.js: {e}", "ERROR")

    return False

# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def run_pipeline(sources: list):
    total = len(sources)
    log_pipeline("FETCH", f"🚀 Pipeline iniciado — {total} fonte(s) para processar")

    articles_found = 0
    articles_saved = 0

    for idx, source in enumerate(sources, 1):
        url = source.get('url')
        name = source.get('name', 'Desconhecida')

        if not url:
            log_pipeline("FETCH", f"⚠️ Fonte {idx}/{total} sem URL definida — pulando", "WARN")
            continue

        log_pipeline("FETCH", f"━━━ Fonte {idx}/{total}: {name} ━━━")
        links = fetch_and_parse(url)

        if not links:
            log_pipeline("FETCH", f"⚠️ Nenhum link encontrado em '{name}'", "WARN")
            continue

        for link_idx, link in enumerate(links, 1):
            log_pipeline("FETCH", f"   🔗 Artigo {link_idx}/{len(links)}: {link[:70]}")
            articles_found += 1

            # 1. Lê o texto do artigo
            raw_text = read_article_text(link)
            if not raw_text or len(raw_text) < 300:
                log_pipeline("FETCH", f"   ⚠️ Conteúdo insuficiente ({len(raw_text)} chars) — pulando", "WARN")
                continue

            # 2. IA reescreve
            ai_output = rewrite_with_ai(raw_text)
            if not ai_output:
                log_pipeline("AI", "   ❌ IA não retornou resultado — pulando artigo", "ERROR")
                continue

            title = ai_output.get("title", "")
            summary_script = ai_output.get("summary", "")
            log_pipeline("AI", f"   📰 Artigo processado: '{title[:50]}'")

            # 3. Gera vídeo
            video_url = None
            if summary_script:
                keywords = " ".join([w for w in title.split() if len(w) > 4][:4])
                log_pipeline("VIDEO", f"   🎬 Gerando vídeo com keywords: '{keywords}'")
                video_url = generate_video_and_upload(summary_script, keywords)
            else:
                log_pipeline("VIDEO", "   ⚠️ Sem roteiro (summary) — vídeo não será gerado", "WARN")

            # 4. Salva no banco
            saved = push_to_nextjs(ai_output, link, video_url)
            if saved:
                articles_saved += 1

    log_pipeline("INGEST", f"🏁 Pipeline finalizado! {articles_saved}/{articles_found} artigo(s) salvos com sucesso", "SUCCESS")

# ─── DAEMON PRINCIPAL ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("═══════════════════════════════════════════════")
    print(" Worker IA — Portal Plugando IA")
    print(" Daemon de coleta, geração e publicação")
    print("═══════════════════════════════════════════════")

    print_env_diagnostics()

    next_auto_run_time = datetime.now()
    first_run = True

    while True:
        try:
            config = get_dynamic_config()
            sources = config.get("sources", [])
            trigger_now = config.get("trigger_now", False)

            # Roda teste de conectividade na primeira iteração
            if first_run:
                test_connectivity()
                first_run = False

            if trigger_now:
                log_pipeline("FETCH", f"⚡ DISPARO MANUAL DETECTADO! {len(sources)} fonte(s) configurada(s)", "INFO")
                run_pipeline(sources)
                next_auto_run_time = datetime.now() + timedelta(hours=INTERVAL_HOURS)
                log_pipeline("FETCH", f"⏰ Próxima execução automática: {next_auto_run_time.strftime('%d/%m %H:%M')}")

            elif datetime.now() >= next_auto_run_time:
                log_pipeline("FETCH", f"🕒 Execução automática periódica — {len(sources)} fonte(s)", "INFO")
                run_pipeline(sources)
                next_auto_run_time = datetime.now() + timedelta(hours=INTERVAL_HOURS)
                log_pipeline("FETCH", f"⏰ Próxima execução automática: {next_auto_run_time.strftime('%d/%m %H:%M')}")

        except Exception as general_error:
            log_pipeline("ERROR", f"💥 ERRO CRÍTICO NO DAEMON: {general_error}", "ERROR")
            log_pipeline("ERROR", f"   {traceback.format_exc()[-500:]}", "ERROR")

        # Dorme 1 minuto e repete
        time.sleep(60)
