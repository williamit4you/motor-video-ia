import os
import asyncio
import time
import requests as std_requests
import re
import boto3
from urllib.parse import urljoin
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
from curl_cffi import requests

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME= os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

s3_client = None
if MINIO_ENDPOINT:
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )
    except Exception as e:
        print("S3 init error:", e)

def upload_to_minio(file_path: str, object_name: str, content_type: str) -> str:
    if not s3_client:
        raise Exception("S3 client not initialized")
    s3_client.upload_file(
        file_path, MINIO_BUCKET_NAME, object_name,
        ExtraArgs={'ContentType': content_type}
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

async def scrape_shopee_product(url: str):
    print(f"[Shopee CFFI] Scraping: {url}")
    
    # curl_cffi bypasses Datadome perfectly because it impersonates Chrome's TLS fingerprint
    r = requests.get(url, impersonate="chrome110")
    html = r.text
    
    # Extrair título
    title = ""
    title_match = re.search(r'<title>(.*?)</title>', html)
    if title_match:
        title = title_match.group(1).replace(" | Shopee Brasil", "").strip()

    # Como não estamos usando navegador completo, extraímos a descrição do meta tag se houver
    desc = ""
    desc_match = re.search(r'<meta name="description" content="(.*?)"', html)
    if desc_match:
        desc = desc_match.group(1).strip()
        
    # Extrair imagens e vídeos do HTML (a Shopee envia tudo embutido no source para SSR)
    images = []
    mp4_urls = []
    
    # Pega URLs de imagens jpeg/png da shopee
    img_matches = re.findall(r'https?:\/\/down-cvs-br\.vod\.susercontent\.com[^\s"\'<>]*(?:\.jpg|\.png|\.webp)', html)
    if not img_matches:
        img_matches = re.findall(r'https?:\/\/cf\.shopee\.com\.br\/file\/[a-zA-Z0-9_]+', html)
        
    for img_url in img_matches:
        clean = img_url.replace('\\', '')
        if clean not in images:
            images.append(clean)
            
    # Procurar por URLs de MP4
    mp4_matches = re.findall(r'https?:\/\/[^\s"\'<>]*\.mp4[^\s"\'<>]*', html)
    for v_url in mp4_matches:
        clean = v_url.replace('\\', '')
        if ".mp4" in clean and clean not in mp4_urls:
            mp4_urls.append(clean)
            
    video = mp4_urls[0] if mp4_urls else None
    
    # Se não conseguimos título pela tag title, damos um fallback
    if not title:
        title = "Produto Shopee"

    uploaded_media = []
    os.makedirs("temp_uploads", exist_ok=True)
    
    # Limit to 5 images max
    for i, img_url in enumerate(images[:5]):
        try:
            res = std_requests.get(img_url, timeout=30)
            if res.status_code == 200:
                ext = "jpg"
                if "png" in img_url: ext = "png"
                elif "webp" in img_url: ext = "webp"
                
                filename = f"temp_uploads/shopee_img_{int(time.time())}_{i}.{ext}"
                with open(filename, "wb") as f:
                    f.write(res.content)
                
                minio_url = upload_to_minio(filename, f"shopee/images/{os.path.basename(filename)}", f"image/{ext}")
                uploaded_media.append({"tipo": "IMAGE", "url": minio_url})
                os.remove(filename)
        except Exception as e:
            print("Error uploading image:", e)
            
    if video:
        try:
            res = std_requests.get(video, timeout=60, stream=True)
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
        "titulo": title.strip() if title else "",
        "descricao": desc.strip() if desc else "",
        "detalhes": desc[:500] if desc else "", 
        "aiPromptVendas": ai_prompt,
        "linksMedia": uploaded_media
    }
