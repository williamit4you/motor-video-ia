import os
import time
import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
import os
import time
import requests
import json
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError

# Busca o .env na raiz (diretório pai)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env') if 'dev' not in __file__ else os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
# Pra simplificar e ser direto baseado no cwd, procuramos nos paths:
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Configurações do seu Motor
NEXT_JS_INGEST_URL = os.environ.get("NEXT_JS_INGEST_URL", "http://localhost:3000/api/worker/ingest")
NEXT_JS_SOURCES_URL = os.environ.get("NEXT_JS_SOURCES_URL", "http://localhost:3000/api/worker/sources")
SECRET_CRON_KEY = os.environ.get("WORKER_SECRET_KEY", "super-secret-worker-key-123")
FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://localhost:8000/gerar-video")

# Configurações MinIO / S3
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

try:
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1' # Default for minio
    )
except Exception as e:
    print("S3 Client init error", e)
    s3_client = None

# Intervalo padrão de varredura automática (em horas)
INTERVAL_HOURS = 6

def get_dynamic_config():
    """ Lê do BackEnd via API QUAIS são os scrapers vivos e SE o usuário apertou Botão Manual! """
    try:
        res = requests.get(NEXT_JS_SOURCES_URL)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print("Erro ao comunicar com a Base do Portal:", e)
    return {"sources": [], "trigger_now": False}

def fetch_and_parse(url):
    print(f"Buscando as notícias de: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print("Falha na varredura. Status:", response.status_code)
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.select('a')[:30]: 
            href = a.get('href')
            if href and ('noticia' in href or 'tecnologia' in href or 'tudo-sobre' in href or 'cnnbrasil' in href or '/tecnologia/' in href):
                # Caso o link seja relativo (ex: /noticia/teste), convertemos para absoluto
                if href.startswith('/'):
                    from urllib.parse import urljoin
                    href = urljoin(url, href)
                links.append(href)
        # Limitando pra n comer os tokens na demo
        return list(set(links))[:3]
    except Exception as e:
        print("Falha térmica ao acessar o Alvo:", e)
        return []

def read_article_text(article_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(article_url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        # Encontra p genéricos para suportar G1, CNN e outros
        paragraphs = soup.find_all('p')
        full_text = " ".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])
        return full_text
    except Exception as e:
        return ""

def rewrite_with_ai(raw_text):
    print("Processando inteligência anti-plágio no LangChain...")
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.7)
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Você é um jornalista independente de tecnologia focado em alta conversão SEO.
Seu objetivo é ler um texto raw raspado da internet, e REESCREVÊ-LO por completo com suas palavras,
garantindo que NENHUM plágio seja detectado, mas mantendo 100% da precisão dos fatos noticiados.
Você deve outputar um JSON rigorosamente estruturado com:
- "title": Um título impactante (SEM clickbait exagerado, formato editorial)
- "summary": Um roteiro ENGAAJADOR e direto de até 30 segundos de locução para um vídeo TikTok/Reels/Story baseado na notícia (máx 450 caracteres).
- "content_html": O artigo escrito, formatado com tags HTML semânticas como <p>, <h2>, e <b>. Formato pronto pro TipTap Editor.
"""),
        ("user", "Texto Original Bruto: {raw_text}")
    ])
    
    chain = prompt | llm | JsonOutputParser()
    try:
        resultado_json = chain.invoke({"raw_text": raw_text[:8000]})
        return resultado_json
    except Exception as e:
        print(f"Erro no processamento do LangChain: {e}")
        return None

def upload_to_minio(file_path, object_name):
    if not s3_client:
        return None
    try:
        s3_client.upload_file(
            file_path, MINIO_BUCKET_NAME, object_name,
            ExtraArgs={'ContentType': 'video/mp4'}
        )
        return f"{MINIO_PUBLIC_URL}/{object_name}"
    except Exception as e:
        print(f"Erro upload S3: {e}")
        return None

def generate_video_and_upload(summary_text):
    print("Gerando video em portrait para Stories...")
    try:
        res = requests.post(FASTAPI_URL, data={
            "text": summary_text,
            "video_format": "portrait",
            "background_color": "#111827",
            "speed": "+5%"
        }, files={"dummy": ("", "")}, timeout=300)
        
        if res.status_code == 200:
            temp_path = f"temp_worker_video_{int(time.time())}.mp4"
            with open(temp_path, "wb") as f:
                f.write(res.content)
            
            s3_url = upload_to_minio(temp_path, f"stories/story_{int(time.time())}.mp4")
            print(f"URL GERADA DO MINIO NO SCRAPER: {s3_url}")
            
            # Limpa local
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
            return s3_url
        else:
            print(f"Erro na API de video (Status {res.status_code}): {res.text}")
    except Exception as e:
        print("Falha ao gerar e subir video:", e)
    
    return None

def push_to_nextjs(article_data, source_url, videoUrl):
    print("Injetando Rascunho no Banco Próprio (Next.js)...")
    payload = {
        "title": article_data.get("title", "Sem título"),
        "summary": article_data.get("summary", "Sem resumo"),
        "content": article_data.get("content_html", ""),
        "sourceUrl": source_url,
        "videoUrl": videoUrl,
        "secret": SECRET_CRON_KEY
    }
    
    try:
        res = requests.post(NEXT_JS_INGEST_URL, json=payload, timeout=20)
        if res.status_code == 200:
            print(f"✅ Inserido com sucesso > {payload['title']}")
        else:
            print(f"⚠️ Erro ao inserir ou Duplicado: {res.text}")
    except Exception as e:
        print(f"❌ Falha de Conexão com Next.js: {e}")

def run_pipeline(sources):
    print("🤖 Iniciando Motor Autônomo Multi-Agente...")
    for source in sources:
        url = source.get('url')
        if not url: continue
        
        links = fetch_and_parse(url)
        print(f"Encontrados {len(links)} links frescos em {source.get('name')}.")
        
        for link in links:
            raw_text = read_article_text(link)
            if not raw_text or len(raw_text) < 300:
                continue
            ai_output = rewrite_with_ai(raw_text)
            if ai_output:
                summary_script = ai_output.get("summary")
                video_url = None
                if summary_script:
                    video_url = generate_video_and_upload(summary_script)
                
                push_to_nextjs(ai_output, link, video_url)

if __name__ == "__main__":
    print(f"Worker em Execução. Este script é um DAEMON feito para containers Docker (Easypanel). Ficará rodando em loop infinito.")
    
    next_auto_run_time = datetime.now()

    while True:
        try:
            # Puxa o painel de comando do Admin via API
            config = get_dynamic_config()
            sources = config.get("sources", [])
            trigger_now = config.get("trigger_now", False)

            if trigger_now:
                print("⚡ SINAL MANUAL DETECTADO! Rodando sob demanda...")
                run_pipeline(sources)
                next_auto_run_time = datetime.now() + timedelta(hours=INTERVAL_HOURS)
                print(f"Próxima puxada natural armada para: {next_auto_run_time.strftime('%H:%M')}")
            
            elif datetime.now() >= next_auto_run_time:
                print("🕒 Hora de rodar pela programação periódica natural...")
                run_pipeline(sources)
                next_auto_run_time = datetime.now() + timedelta(hours=INTERVAL_HOURS)
                print(f"Terminou. Nova puxada armada para: {next_auto_run_time.strftime('%H:%M')}")
            
        except Exception as general_error:
            print("Erro Global no Daemon:", general_error)
        
        # Dorme 1 minuto e checa o painel do Admin de novo.
        time.sleep(60)

