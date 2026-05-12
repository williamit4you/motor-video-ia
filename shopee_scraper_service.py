import os
import re
import time
import requests as std_requests
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from curl_cffi import requests

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

MINIO_ENDPOINT   = os.environ.get("MINIO_INTERNAL_ENDPOINT") or os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME= os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

print(
    "[worker][s3] init",
    {
        "endpoint": MINIO_ENDPOINT or "MISSING",
        "bucket": MINIO_BUCKET_NAME,
        "public_url": MINIO_PUBLIC_URL or "MISSING",
        "access_key_present": bool(MINIO_ACCESS_KEY),
        "secret_key_present": bool(MINIO_SECRET_KEY),
    }
)

s3_client = None
if MINIO_ENDPOINT:
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1',
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )
    except Exception as e:
        print("S3 init error:", e)

def upload_to_minio(file_path: str, object_name: str, content_type: str) -> str:
    if not s3_client:
        raise Exception("S3 client not initialized")
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else -1
    started = time.time()
    print(
        "[worker][s3] upload:start",
        {
            "endpoint": MINIO_ENDPOINT or "MISSING",
            "bucket": MINIO_BUCKET_NAME,
            "object_name": object_name,
            "content_type": content_type,
            "file_size": file_size,
        }
    )
    with open(file_path, "rb") as f:
        s3_client.put_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=object_name,
            Body=f,
            ContentType=content_type,
        )
    elapsed = round(time.time() - started, 2)
    print(
        "[worker][s3] upload:done",
        {
            "object_name": object_name,
            "elapsed_sec": elapsed,
            "public_url": f"{MINIO_PUBLIC_URL}/{object_name}",
        }
    )
    return f"{MINIO_PUBLIC_URL}/{object_name}"

def generate_sales_prompt(title: str, description: str) -> str:
    system_prompt = """Você é um especialista em marketing digital e copy de vendas.
Seu objetivo é criar um script de vendas curto e persuasivo sobre um produto da Shopee.
Você receberá o Título e a Descrição do produto.
Crie um texto com gatilhos mentais para um vendedor falar em um vídeo (estilo Reels/TikTok),
explicando os benefícios e características principais do produto.
MUITO IMPORTANTE: O script DEVE sempre terminar com uma variação da frase: "Para ter acesso ao produto, o link está na bio!".
Devolva um JSON estrito:
{
  "script_vendas": "Seu texto aqui..."
}
"""
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    llm = ChatOpenAI(model=model_name, temperature=0.7)
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", f"Título: {title}\nDescrição: {description}")
    ])
    chain = prompt | llm | JsonOutputParser()
    try:
        res = chain.invoke({})
        return res.get("script_vendas", "")
    except:
        return ""

def _extract_shopee_raw(url: str) -> dict:
    """
    Uses curl_cffi to bypass Datadome TLS fingerprint detection.
    Returns raw (not yet uploaded) video/image URLs from the Shopee product page.
    This function does NOT upload to MinIO — the caller is responsible for that.
    """
    print(f"[Shopee CFFI] Fetching: {url}")

    # curl_cffi impersonates Chrome 110 TLS fingerprint — bypasses Datadome on cloud IPs
    r = requests.get(
        url,
        impersonate="chrome110",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "accept-encoding": "gzip, deflate, br",
            "referer": "https://shopee.com.br/",
        },
        timeout=30
    )
    html = r.text
    print(f"[Shopee CFFI] HTML length: {len(html)}, status: {r.status_code}")

    # Extract title — og:title is the real product name; <title> is often "Shopping Cart Icon"
    title = ""
    for pattern in [
        r'<meta\s+property="og:title"\s+content="([^"]+)"',
        r'<meta\s+name="og:title"\s+content="([^"]+)"',
        r'"name"\s*:\s*"([^"]{10,}?)"',   # JSON-LD product name fallback
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            # Skip generic titles
            if candidate and "Shopping Cart" not in candidate and "Shopee" not in candidate:
                title = re.sub(r'\s*[\|\-]\s*Shopee.*$', '', candidate, flags=re.IGNORECASE).strip()
                break

    # If still empty, try <title> as last resort
    if not title:
        m = re.search(r'<title>(.*?)</title>', html)
        if m:
            title = re.sub(r'\s*[\|\-]\s*Shopee.*$', m.group(1), flags=re.IGNORECASE).strip()

    # Extract description from meta tags
    desc = ""
    for pattern in [
        r'<meta\s+property="og:description"\s+content="([^"]*)"',
        r'<meta\s+name="description"\s+content="([^"]*)"',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            desc = m.group(1).strip()
            break

    # Extract MP4 video URLs (Shopee embeds them in SSR HTML)
    mp4_urls = []
    for raw in re.findall(r'https?://[^\s"\'<>]*\.mp4[^\s"\'<>]*', html):
        clean = raw.replace('\\u002F', '/').replace('\\/', '/').rstrip('\\')
        if '.mp4' in clean and clean not in mp4_urls:
            mp4_urls.append(clean)

    # Extract image URLs from susercontent CDN — explicitly EXCLUDE .mp4 files
    img_urls = []
    for pattern in [
        r'(https?://down-[a-z]+-[a-z]+\.img\.susercontent\.com/file/[a-zA-Z0-9_\-]+)',
        r'(https?://[a-z0-9\-]+\.img\.susercontent\.com/file/[a-zA-Z0-9_\-]+)',
        r'(https?://cf\.shopee\.com\.br/file/[a-zA-Z0-9_\-]+)',
    ]:
        for raw in re.findall(pattern, html):
            clean = raw.replace('\\u002F', '/').replace('\\/', '/').rstrip('\\')
            # Skip if it's actually an mp4 URL
            if '.mp4' not in clean and clean not in img_urls:
                img_urls.append(clean)

    video_url = mp4_urls[0] if mp4_urls else None

    print(f"[Shopee CFFI] Title: '{title}' | Video: {bool(video_url)} | Images: {len(img_urls)}")

    return {
        "titulo": title,
        "descricao": desc,
        "videoRawUrl": video_url,
        "imageRawUrls": img_urls[:5],
    }

async def scrape_shopee_raw(url: str) -> dict:
    """
    Endpoint for render-service: returns raw URLs without MinIO upload.
    The render-service (which has MinIO creds) handles the upload.
    """
    return _extract_shopee_raw(url)

async def scrape_shopee_product(url: str):
    """
    Full pipeline: extract raw URLs + upload to MinIO + generate AI script.
    Used by the /scraping-shopee FastAPI endpoint in video.py.
    """
    raw = _extract_shopee_raw(url)
    title = raw["titulo"] or "Produto Shopee"
    desc = raw["descricao"]
    video_url = raw["videoRawUrl"]
    img_urls = raw["imageRawUrls"]

    uploaded_media = []
    os.makedirs("temp_uploads", exist_ok=True)

    # Upload images to MinIO
    for i, img_url in enumerate(img_urls):
        try:
            res = std_requests.get(img_url, timeout=30)
            if res.status_code == 200:
                ext = "webp" if "webp" in img_url else "png" if "png" in img_url else "jpg"
                filename = f"temp_uploads/shopee_img_{int(time.time())}_{i}.{ext}"
                with open(filename, "wb") as f:
                    f.write(res.content)
                minio_url = upload_to_minio(filename, f"shopee/images/{os.path.basename(filename)}", f"image/{ext}")
                uploaded_media.append({"tipo": "IMAGE", "url": minio_url})
                os.remove(filename)
        except Exception as e:
            print("Error uploading image:", e)

    # Upload video to MinIO
    if video_url:
        try:
            res = std_requests.get(video_url, timeout=90, stream=True)
            if res.status_code == 200:
                filename = f"temp_uploads/shopee_vid_{int(time.time())}.mp4"
                with open(filename, "wb") as f:
                    for chunk in res.iter_content(chunk_size=65536):
                        f.write(chunk)
                minio_url = upload_to_minio(filename, f"shopee/videos/{os.path.basename(filename)}", "video/mp4")
                uploaded_media.append({"tipo": "VIDEO", "url": minio_url})
                os.remove(filename)
        except Exception as e:
            print("Error uploading video:", e)

    try:
        ai_prompt = generate_sales_prompt(title, desc)
    except Exception as e:
        print("Error generating AI prompt:", e)
        ai_prompt = ""

    return {
        "titulo": title.strip(),
        "descricao": desc.strip(),
        "detalhes": desc[:500],
        "aiPromptVendas": ai_prompt,
        "linksMedia": uploaded_media
    }
