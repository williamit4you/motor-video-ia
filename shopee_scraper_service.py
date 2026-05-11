import os
import asyncio
import time
import requests
import boto3
from urllib.parse import urljoin
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv
from playwright.async_api import async_playwright

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
    res = chain.invoke({})
    return res.get("script_vendas", "")

async def scrape_shopee_product(url: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)
        
        try:
            title = await page.locator("div[class*='page-product'] h1, h1").first.text_content()
        except:
            title = await page.title()
            
        try:
            desc = await page.locator("div[class*='product-detail']").text_content()
            if not desc:
                desc = await page.locator("div[style*='white-space: pre-wrap']").first.text_content()
        except:
            desc = ""
            
        try:
            images = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('picture img')).map(img => img.src).filter(src => src.includes('http'));
            }""")
            images = list(dict.fromkeys(images)) # remove duplicates
        except:
            images = []
            
        try:
            video = await page.evaluate("""() => {
                let v = document.querySelector('video');
                return v ? v.src : null;
            }""")
        except:
            video = None
            
        await browser.close()
        
        # Download and upload media
        uploaded_media = []
        os.makedirs("temp_uploads", exist_ok=True)
        
        # Limit to 5 images max
        for i, img_url in enumerate(images[:5]):
            try:
                res = requests.get(img_url, timeout=30)
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
                res = requests.get(video, timeout=60, stream=True)
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
            "detalhes": desc[:500] if desc else "", # Using part of desc as details
            "aiPromptVendas": ai_prompt,
            "linksMedia": uploaded_media
        }
