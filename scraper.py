import os
import time
import requests
import json
import traceback
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.callbacks.manager import get_openai_callback
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
NEXT_JS_CONFIG_URL = f"{NEXT_JS_BASE_URL}/api/worker/config"
NEXT_JS_RUNS_URL   = f"{NEXT_JS_BASE_URL}/api/worker/runs"
NEXT_JS_AI_USAGE_URL = f"{NEXT_JS_BASE_URL}/api/worker/ai-usage"
SECRET_CRON_KEY    = os.environ.get("WORKER_SECRET_KEY", "super-secret-worker-key-123")
FASTAPI_URL        = os.environ.get("FASTAPI_URL", "http://localhost:8000/gerar-video")
PEXELS_API_KEY     = os.environ.get("PEXELS_API_KEY", "")

# ─── CONFIGURAÇÕES MINIO / S3 ─────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME= os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

# ─── PREÇOS OPENAI (USD por 1M tokens) ───────────────────────────────────────
# Fonte: https://openai.com/pricing — atualizar conforme necessário
OPENAI_PRICING = {
    "gpt-4o":        {"input": 2.50,  "output": 10.00},
    "gpt-4o-mini":   {"input": 0.15,  "output": 0.60},
    "gpt-4-turbo":   {"input": 10.00, "output": 30.00},
    "gpt-4":         {"input": 30.00, "output": 60.00},
    "gpt-3.5-turbo": {"input": 0.50,  "output": 1.50},
}

# ─── PROMPT PADRÃO (fallback se o banco não tiver config) ────────────────────
DEFAULT_SYSTEM_PROMPT = """Você é um jornalista independente de tecnologia focado em alta conversão SEO.
Seu objetivo é ler um texto raw raspado da internet, e REESCREVÊ-LO por completo com suas palavras,
garantindo que NENHUM plágio seja detectado, mas mantendo 100% da precisão dos fatos noticiados.
Você deve outputar um JSON rigorosamente estruturado com:
- "title": Um título impactante (SEM clickbait exagerado, formato editorial)
- "summary": Um roteiro ENGAJADOR e direto de até {duration_sec} segundos de locução para um vídeo TikTok/Reels/Story baseado na notícia (máx 450 caracteres).
- "content_html": O artigo escrito, formatado com tags HTML semânticas como <p>, <h2>, e <b>. Formato pronto pro TipTap Editor.
{style_instruction}"""

# ─── INSTRUÇÕES DE ESTILO ─────────────────────────────────────────────────────
STYLE_INSTRUCTIONS = {
    "journalism":    "Escreva de forma jornalística: informativo, direto, objetivo.",
    "story":         "Escreva como uma história: narrativo, envolvente, com início, meio e fim.",
    "ad":            "Escreva como propaganda: persuasivo, apelativo, focado em benefícios.",
    "funny":         "Escreva de forma divertida: com humor, descontraído, mas informativo.",
    "ironic":        "Escreva de forma irônica: crítico, sarcástico, mas embasado nos fatos.",
    "polemico":      "Escreva de forma POLÊMICA e viral: provocador, com gatilhos emocionais (indignação, surpresa, curiosidade). Gancho forte nos primeiros 3 segundos. Foco máximo em compartilhamento.",
    "breaking":      "Escreva como breaking news AO VIVO: urgência máxima, boletim direto, sensação de última hora. Use linguagem de telejornal em estado de alerta.",
    "investigativo": "Escreva de forma investigativa: revele o que está por trás da notícia, questione, aprofunde. Tom de suspense jornalístico. Mostre contextos ocultos e impactos não óbvios.",
}

# ─── CONFIG PADRÃO DE FALLBACK ────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "intervalHours": 6,
    "scheduledTimes": "[]",
    "useScheduledTimes": False,
    "isEnabled": True,
    "maxArticlesPerRun": 3,
    "aiModel": "gpt-4o-mini",
    "aiTemperature": 0.7,
    "systemPrompt": DEFAULT_SYSTEM_PROMPT,
    "videoDurationSec": 30,
    "videoStyle": "journalism",
    "ttsVoice": "pt-BR-AntonioNeural",
    "ttsSpeed": "+5%",
    "pexelsEnabled": True,
    "autoPublishReels": False,
    "autoPublishStory": False,
    "autoPublishTikTok": False,
    "autoPublishLinkedIn": False,
}

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

def get_scraper_config() -> dict:
    """Busca configurações dinâmicas do painel admin. Usa fallback seguro se falhar."""
    try:
        res = requests.get(
            NEXT_JS_CONFIG_URL,
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=5
        )
        if res.status_code == 200:
            cfg = res.json()
            log_pipeline("CONFIG", f"✅ Config carregada: model={cfg.get('aiModel')}, "
                         f"maxArticles={cfg.get('maxArticlesPerRun')}, "
                         f"isEnabled={cfg.get('isEnabled')}, "
                         f"style={cfg.get('videoStyle')}", "INFO")
            return cfg
        else:
            log_pipeline("CONFIG", f"⚠️ HTTP {res.status_code} ao buscar config — usando padrão", "WARN")
    except Exception as e:
        log_pipeline("CONFIG", f"⚠️ Falha ao buscar config ({e}) — usando padrão", "WARN")
    return DEFAULT_CONFIG.copy()

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

# ─── GERENCIAMENTO DE EXECUÇÕES ───────────────────────────────────────────────

def create_run_record(trigger_type: str = "AUTO") -> str | None:
    """Cria um registro de execução no banco e retorna o ID."""
    try:
        res = requests.post(
            NEXT_JS_RUNS_URL,
            json={"triggerType": trigger_type, "startedAt": datetime.now().isoformat()},
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=5
        )
        if res.status_code == 200:
            run_id = res.json().get("id")
            log_pipeline("RUN", f"📋 Execução registrada: ID={run_id}")
            return run_id
    except Exception as e:
        log_pipeline("RUN", f"⚠️ Falha ao registrar execução: {e}", "WARN")
    return None

def finish_run_record(run_id: str | None, status: str, found: int, saved: int,
                      tokens_in: int = 0, tokens_out: int = 0, cost_usd: float = 0.0,
                      error: str | None = None):
    """Fecha o registro de execução com os resultados finais."""
    if not run_id:
        return
    try:
        requests.patch(
            f"{NEXT_JS_RUNS_URL}/{run_id}",
            json={
                "status": status,
                "articlesFound": found,
                "articlesSaved": saved,
                "finishedAt": datetime.now().isoformat(),
                "totalTokensIn": tokens_in,
                "totalTokensOut": tokens_out,
                "totalCostUsd": cost_usd,
                "errorMessage": error,
            },
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=5
        )
    except Exception as e:
        log_pipeline("RUN", f"⚠️ Falha ao fechar registro de execução: {e}", "WARN")

# ─── RASTREAMENTO DE CUSTOS DE IA ─────────────────────────────────────────────

def calculate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Calcula o custo em USD baseado no modelo e tokens usados."""
    prices = OPENAI_PRICING.get(model, {"input": 0.0, "output": 0.0})
    cost = (prompt_tokens / 1_000_000) * prices["input"]
    cost += (completion_tokens / 1_000_000) * prices["output"]
    return round(cost, 8)

def log_ai_usage(run_id: str | None, post_id: str | None, operation: str,
                 model: str, prompt_tokens: int, completion_tokens: int,
                 cost_usd: float, input_summary: str = "", output_summary: str = ""):
    """Envia o registro de uso de IA para o Next.js."""
    try:
        requests.post(
            NEXT_JS_AI_USAGE_URL,
            json={
                "runId": run_id,
                "postId": post_id,
                "operation": operation,
                "model": model,
                "promptTokens": prompt_tokens,
                "completionTokens": completion_tokens,
                "totalTokens": prompt_tokens + completion_tokens,
                "costUsd": cost_usd,
                "inputSummary": input_summary[:200] if input_summary else "",
                "outputSummary": output_summary[:200] if output_summary else "",
            },
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=5
        )
    except Exception as e:
        log_pipeline("AI_COST", f"⚠️ Falha ao registrar uso IA: {e}", "WARN")

# ─── AGENDAMENTO ──────────────────────────────────────────────────────────────

def should_run_now(config: dict, next_auto_run_time: datetime) -> bool:
    """Verifica se deve executar agora baseado na config de agendamento."""
    use_scheduled = config.get("useScheduledTimes", False)
    try:
        scheduled_times = json.loads(config.get("scheduledTimes", "[]"))
    except Exception:
        scheduled_times = []

    if use_scheduled and scheduled_times:
        current_hhmm = datetime.now().strftime("%H:%M")
        return current_hhmm in scheduled_times
    else:
        return datetime.now() >= next_auto_run_time

# ─── SCRAPING ─────────────────────────────────────────────────────────────────

def is_article_link(href: str, base_url: str) -> bool:
    """Retorna True se o link parece ser um artigo real, não uma seção/paginador."""
    # Exclui paginadores
    if '/pagina/' in href or '/page/' in href:
        return False
    # Exclui seções puras (poucos segmentos de path)
    from urllib.parse import urlparse
    parsed = urlparse(href)
    path_parts = [p for p in parsed.path.strip('/').split('/') if p]
    # Artigos geralmente têm ≥2 segmentos e o último é um slug (contém hífen ou número)
    if len(path_parts) < 2:
        return False
    last_part = path_parts[-1]
    if '-' not in last_part and not any(c.isdigit() for c in last_part):
        return False
    return True

def fetch_rss(url: str) -> list:
    """Tenta parsear a URL como RSS/Atom feed. Retorna lista de links de artigos."""
    try:
        import xml.etree.ElementTree as ET
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            return []
        root = ET.fromstring(res.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        links = []
        # RSS 2.0
        for item in root.findall('.//item'):
            link = item.findtext('link')
            if link and link.startswith('http'):
                links.append(link.strip())
        # Atom
        for entry in root.findall('.//atom:entry', ns):
            link_el = entry.find('atom:link', ns)
            if link_el is not None:
                href = link_el.get('href', '')
                if href.startswith('http'):
                    links.append(href.strip())
        return links[:5]
    except Exception:
        return []

def fetch_and_parse(url: str, max_articles: int = 3) -> list:
    """Acessa a página-fonte e extrai links de artigos reais."""
    log_pipeline("FETCH", f"🌐 Acessando fonte: {url}")
    
    # Tenta RSS primeiro (mais confiável que scraping)
    rss_links = fetch_rss(url)
    if rss_links:
        log_pipeline("FETCH", f"📡 RSS detectado! {len(rss_links)} artigo(s) encontrados")
        return rss_links[:max_articles]
    
    # Fallback: scraping HTML
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        log_pipeline("FETCH", f"📡 HTTP {response.status_code} — {len(response.text)} chars recebidos")

        if response.status_code != 200:
            log_pipeline("FETCH", f"❌ Falha HTTP {response.status_code} ao acessar {url}", "ERROR")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        all_links = soup.select('a[href]')
        log_pipeline("FETCH", f"🔗 {len(all_links)} links totais encontrados na página")

        ARTICLE_KEYWORDS = [
            '/noticia/', '/noticias/', '/tecnologia/', '/tech/', 
            '/ia/', '/inteligencia', '/tudo-sobre/', '/mundo/',
            '/economia/', '/ciencia/', '/inovacao/'
        ]

        links = []
        for a in all_links:
            href = a.get('href', '')
            if not href:
                continue
            # Normaliza URL
            if href.startswith('/'):
                href = urljoin(url, href)
            if not href.startswith('http'):
                continue
            # Verifica keywords
            if not any(kw in href for kw in ARTICLE_KEYWORDS):
                continue
            # Verifica se é artigo real (não paginador/seção)
            if not is_article_link(href, url):
                continue
            links.append(href)

        unique = list(dict.fromkeys(links))[:max_articles]  # preserva ordem, remove duplas
        log_pipeline("FETCH", f"✅ {len(unique)} artigo(s) válido(s) selecionados")
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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://www.google.com/",
    }
    try:
        res = requests.get(article_url, headers=headers, timeout=15)
        log_pipeline("FETCH", f"📡 HTTP {res.status_code} — {len(res.text)} chars do artigo")

        if res.status_code == 403:
            log_pipeline("FETCH", f"❌ HTTP 403 — site bloqueia bots. Tente adicionar o RSS dessa fonte.", "ERROR")
            return ""
        if res.status_code != 200:
            log_pipeline("FETCH", f"❌ HTTP {res.status_code} ao ler artigo", "ERROR")
            return ""

        soup = BeautifulSoup(res.text, "html.parser")

        # Remove ruído navegacional
        for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
            tag.decompose()

        # Tenta extrair pelo article tag primeiro
        article_tag = soup.find('article')
        source = article_tag if article_tag else soup

        paragraphs = source.find_all('p')
        full_text = " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30])

        log_pipeline("FETCH", f"📄 Texto extraído: {len(full_text)} caracteres, {len(paragraphs)} parágrafos")

        if len(full_text) < 300:
            log_pipeline("FETCH", f"⚠️ Texto curto ({len(full_text)} chars) — paywall ou página vazia", "WARN")

        return full_text

    except requests.Timeout:
        log_pipeline("FETCH", f"❌ Timeout ao ler artigo {article_url[:60]}", "ERROR")
        return ""
    except Exception as e:
        log_pipeline("FETCH", f"❌ Erro ao ler artigo: {e}", "ERROR")
        return ""

# ─── IA ───────────────────────────────────────────────────────────────────────

def rewrite_with_ai(raw_text: str, config: dict, run_id: str | None = None):
    """Usa a IA configurada para reescrever o artigo e gerar o roteiro."""
    model_name = config.get("aiModel", "gpt-4o-mini")
    temperature = config.get("aiTemperature", 0.7)
    video_style = config.get("videoStyle", "journalism")
    duration_sec = config.get("videoDurationSec", 30)
    
    # Monta o prompt com estilo e duração dinâmicos
    base_prompt = config.get("systemPrompt") or DEFAULT_SYSTEM_PROMPT
    style_instruction = STYLE_INSTRUCTIONS.get(video_style, STYLE_INSTRUCTIONS["journalism"])
    system_prompt = base_prompt.replace("{duration_sec}", str(duration_sec)).replace("{style_instruction}", style_instruction)
    
    log_pipeline("AI", f"🤖 Modelo: {model_name} | Estilo: {video_style} | Duração alvo: {duration_sec}s")
    log_pipeline("AI", f"🤖 Enviando {len(raw_text[:8000])} chars para {model_name}...")

    llm = ChatOpenAI(model=model_name, temperature=temperature)

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", "Texto Original Bruto: {raw_text}")
    ])

    chain = prompt | llm | JsonOutputParser()
    try:
        log_pipeline("AI", "⏳ Aguardando resposta da OpenAI...")
        
        with get_openai_callback() as cb:
            resultado = chain.invoke({"raw_text": raw_text[:8000]})
            
            # Captura tokens e calcula custo
            prompt_tokens = cb.prompt_tokens
            completion_tokens = cb.completion_tokens
            cost = calculate_cost(model_name, prompt_tokens, completion_tokens)
            
            log_pipeline("AI", f"💰 Tokens: {prompt_tokens}in + {completion_tokens}out = {cb.total_tokens} total | Custo: US$ {cost:.6f}", "SUCCESS")
            
            # Registra no banco
            log_ai_usage(
                run_id=run_id,
                post_id=None,
                operation="rewrite_article",
                model=model_name,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost_usd=cost,
                input_summary=raw_text[:200],
                output_summary=str(resultado.get("title", "") if resultado else "")[:200],
            )

        if not resultado:
            log_pipeline("AI", "❌ IA retornou resultado vazio", "ERROR")
            return None, 0, 0, 0.0

        title = resultado.get('title', 'sem título')
        summary = resultado.get('summary', '')
        content = resultado.get('content_html', '')

        log_pipeline("AI", f"✅ Título gerado: {title[:70]}", "SUCCESS")
        log_pipeline("AI", f"📝 Resumo (roteiro): {summary[:80]}...")
        log_pipeline("AI", f"📄 Artigo HTML: {len(content)} chars gerados")

        return resultado, prompt_tokens, completion_tokens, cost

    except Exception as e:
        log_pipeline("AI", f"❌ Erro na chamada OpenAI: {e}", "ERROR")
        log_pipeline("AI", f"   Traceback: {traceback.format_exc()[-300:]}", "ERROR")
        return None, 0, 0, 0.0

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

def generate_video_and_upload(summary_text: str, keywords: str = "", config: dict = None) -> str | None:
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    tts_speed = config.get("ttsSpeed", "+5%")
    tts_voice = config.get("ttsVoice", "pt-BR-AntonioNeural")
    pexels_enabled = config.get("pexelsEnabled", True)
    
    log_pipeline("VIDEO", f"🎬 Iniciando geração de vídeo portrait...")
    log_pipeline("VIDEO", f"📝 Roteiro ({len(summary_text)} chars): {summary_text[:80]}...")
    log_pipeline("VIDEO", f"🎙️ Voz: {tts_voice} | Velocidade: {tts_speed} | Pexels: {'✅' if pexels_enabled else '❌'}")

    # Busca fundo Pexels (se habilitado na config)
    pexels_paths = []
    if keywords and pexels_enabled:
        log_pipeline("VIDEO", f"🔑 Keywords para Pexels: '{keywords}'")
        pexels_paths = fetch_pexels_media(keywords)
    elif not pexels_enabled:
        log_pipeline("VIDEO", "⚠️ Pexels desabilitado na config — usando fundo sólido", "WARN")
    else:
        log_pipeline("VIDEO", "⚠️ Sem keywords — usando fundo preto", "WARN")

    try:
        data_fields = {
            "text": summary_text,
            "video_format": "portrait",
            "background_color": "#111827",
            "speed": tts_speed,
            "voice": tts_voice,
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
            resp_data = res.json()
            log_pipeline("INGEST", f"✅ Notícia salva com sucesso: '{title[:60]}'", "SUCCESS")
            return True, {
                "postId": resp_data.get("post"),
                "socialPostId": resp_data.get("socialPostId"),
            }
        elif res.status_code == 409:
            log_pipeline("INGEST", f"⚠️ Notícia duplicada (já existe): {source_url[:60]}", "WARN")
        else:
            log_pipeline("INGEST", f"❌ Erro ao salvar: HTTP {res.status_code} — {res.text[:100]}", "ERROR")

    except requests.Timeout:
        log_pipeline("INGEST", f"❌ Timeout ao chamar Next.js ingest", "ERROR")
    except Exception as e:
        log_pipeline("INGEST", f"❌ Falha de conexão com Next.js: {e}", "ERROR")

    return False, None


# ─── AUTO-PUBLICAÇÃO NAS PLATAFORMAS ────────────────────────────────────────────────

def auto_publish(social_post_id: str, endpoint: str) -> bool:
    """É chamado após criação do vídeo para publicar automaticamente numa plataforma."""
    try:
        url = f"{NEXT_JS_BASE_URL}{endpoint}"
        res = requests.post(
            url,
            json={"socialPostId": social_post_id},
            headers={"x-worker-secret": SECRET_CRON_KEY},
            timeout=60
        )
        if res.ok:
            log_pipeline("AUTO-PUBLISH", f"\u2705 {endpoint} → OK (socialPostId={social_post_id})", "SUCCESS")
            return True
        else:
            log_pipeline("AUTO-PUBLISH", f"\u274c {endpoint} → HTTP {res.status_code}: {res.text[:120]}", "ERROR")
            return False
    except Exception as e:
        log_pipeline("AUTO-PUBLISH", f"\u274c {endpoint} → Erro: {e}", "ERROR")
        return False


def auto_publish_to_platforms(social_post_id: str | None, config: dict):
    """Publica automaticamente nas plataformas configuradas via flags no ScraperConfig."""
    if not social_post_id:
        log_pipeline("AUTO-PUBLISH", "⚠️ Sem social_post_id — pulando auto-publicação", "WARN")
        return

    if config.get("autoPublishReels"):
        log_pipeline("AUTO-PUBLISH", "📹 Auto-publicando como Reels (Meta)...")
        auto_publish(social_post_id, "/api/social/publish")

    if config.get("autoPublishStory"):
        log_pipeline("AUTO-PUBLISH", "📸 Auto-publicando como Story 24h (Meta)...")
        auto_publish(social_post_id, "/api/social/publish-story")

    if config.get("autoPublishTikTok"):
        log_pipeline("AUTO-PUBLISH", "🎵 Auto-publicando no TikTok...")
        auto_publish(social_post_id, "/api/social/publish-tiktok")

    if config.get("autoPublishLinkedIn"):
        log_pipeline("AUTO-PUBLISH", "💼 Auto-publicando no LinkedIn...")
        auto_publish(social_post_id, "/api/social/publish-linkedin")


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def run_pipeline(sources: list, config: dict, trigger_type: str = "AUTO"):
    total = len(sources)
    max_articles = config.get("maxArticlesPerRun", 3)
    log_pipeline("FETCH", f"🚀 Pipeline iniciado — {total} fonte(s) | máx {max_articles} artigos por fonte")

    # Registra a execução no banco
    run_id = create_run_record(trigger_type)
    
    articles_found = 0
    articles_saved = 0
    total_tokens_in = 0
    total_tokens_out = 0
    total_cost = 0.0

    for idx, source in enumerate(sources, 1):
        url = source.get('url')
        name = source.get('name', 'Desconhecida')

        if not url:
            log_pipeline("FETCH", f"⚠️ Fonte {idx}/{total} sem URL definida — pulando", "WARN")
            continue

        log_pipeline("FETCH", f"━━━ Fonte {idx}/{total}: {name} ━━━")
        links = fetch_and_parse(url, max_articles)

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

            # 2. IA reescreve (retorna tokens e custo)
            ai_output, tokens_in, tokens_out, cost = rewrite_with_ai(raw_text, config, run_id)
            total_tokens_in += tokens_in
            total_tokens_out += tokens_out
            total_cost += cost
            
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
                video_url = generate_video_and_upload(summary_script, keywords, config)
            else:
                log_pipeline("VIDEO", "   ⚠️ Sem roteiro (summary) — vídeo não será gerado", "WARN")

            # 4. Salva no banco
            saved, saved_ids = push_to_nextjs(ai_output, link, video_url)
            if saved:
                articles_saved += 1
                # saved_ids pode ser post_id ou dict com post_id e social_post_id
                social_post_id = None
                if isinstance(saved_ids, dict):
                    social_post_id = saved_ids.get("socialPostId")
                # 5. Auto-publicar nas plataformas configuradas
                auto_publish_to_platforms(social_post_id, config)

    run_status = "SUCCESS" if articles_saved > 0 else ("FAILED" if articles_found == 0 else "PARTIAL")
    log_pipeline("INGEST", f"🏁 Pipeline finalizado! {articles_saved}/{articles_found} artigo(s) salvos | "
                 f"💰 Custo total: US$ {total_cost:.6f} | Tokens: {total_tokens_in + total_tokens_out}", "SUCCESS")
    
    # Fecha registro de execução
    finish_run_record(run_id, run_status, articles_found, articles_saved,
                      total_tokens_in, total_tokens_out, total_cost)

# ─── DAEMON PRINCIPAL ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("═══════════════════════════════════════════════")
    print(" Worker IA — Portal Plugando IA")
    print(" Daemon de coleta, geração e publicação")
    print("═══════════════════════════════════════════════")

    print_env_diagnostics()

    # Carrega config inicial
    scraper_config = get_scraper_config()
    interval_hours = scraper_config.get("intervalHours", 6)

    # Primeira execução automática só após INTERVAL_HOURS
    # (evita rodar imediatamente ao reiniciar o container)
    next_auto_run_time = datetime.now() + timedelta(hours=interval_hours)

    first_run = True
    last_scheduled_minute = ""  # Controla que horários fixos não disparem mais de uma vez no mesmo minuto

    while True:
        try:
            # Atualiza config a cada ciclo (permite mudanças sem reiniciar)
            scraper_config = get_scraper_config()
            sources_data = get_dynamic_config()
            sources = sources_data.get("sources", [])
            trigger_now = sources_data.get("trigger_now", False)

            # Atualiza intervalo se mudou na config
            interval_hours = scraper_config.get("intervalHours", 6)

            # Roda teste de conectividade na primeira iteração
            if first_run:
                test_connectivity()
                first_run = False

            # Verifica se coleta automática está habilitada
            is_enabled = scraper_config.get("isEnabled", True)
            if not is_enabled and not trigger_now:
                next_run_str = next_auto_run_time.strftime('%d/%m %H:%M')
                log_pipeline("DAEMON", f"⏸️ Coleta automática DESABILITADA pelo admin. Aguardando reativação...")
                time.sleep(60)
                continue

            current_minute = datetime.now().strftime("%H:%M")

            if trigger_now:
                log_pipeline("FETCH", f"⚡ DISPARO MANUAL DETECTADO! {len(sources)} fonte(s) configurada(s)", "INFO")
                run_pipeline(sources, scraper_config, "MANUAL")
                next_auto_run_time = datetime.now() + timedelta(hours=interval_hours)
                log_pipeline("FETCH", f"⏰ Próxima execução automática: {next_auto_run_time.strftime('%d/%m %H:%M')}")

            elif should_run_now(scraper_config, next_auto_run_time) and current_minute != last_scheduled_minute:
                use_scheduled = scraper_config.get("useScheduledTimes", False)
                mode_label = "por horário fixo" if use_scheduled else "periódica"
                log_pipeline("FETCH", f"🕒 Execução automática {mode_label} — {len(sources)} fonte(s)", "INFO")
                run_pipeline(sources, scraper_config, "AUTO")
                last_scheduled_minute = current_minute
                if not use_scheduled:
                    next_auto_run_time = datetime.now() + timedelta(hours=interval_hours)
                    log_pipeline("FETCH", f"⏰ Próxima execução automática: {next_auto_run_time.strftime('%d/%m %H:%M')}")
            else:
                # Log apenas uma vez por hora para não poluir
                if datetime.now().minute == 0:
                    remaining = next_auto_run_time - datetime.now()
                    mins = int(remaining.total_seconds() / 60)
                    log_pipeline("DAEMON", f"😴 Aguardando... próx. coleta em {mins} min")

        except Exception as general_error:
            log_pipeline("ERROR", f"💥 ERRO CRÍTICO NO DAEMON: {general_error}", "ERROR")
            log_pipeline("ERROR", f"   {traceback.format_exc()[-500:]}", "ERROR")

        # Dorme 1 minuto e repete
        time.sleep(60)
