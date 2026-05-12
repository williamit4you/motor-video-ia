import re
import json
import os
import time
import requests as std_requests
from curl_cffi import requests
from urllib.parse import urlparse

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "uploads")
MINIO_PUBLIC_URL = os.environ.get("MINIO_PUBLIC_URL")

s3_client = None
if MINIO_ENDPOINT:
    import boto3
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )
    except:
        pass

def upload_to_minio(file_path: str, object_name: str, content_type: str) -> str:
    if not s3_client:
        raise Exception("S3 client not initialized")
    s3_client.upload_file(
        file_path, MINIO_BUCKET_NAME, object_name,
        ExtraArgs={'ContentType': content_type}
    )
    return f"{MINIO_PUBLIC_URL}/{object_name}"

def scrape_shopee_video(product_url: str):
    print(f"[Shopee CFFI] Scraping: {product_url}")
    
    # curl_cffi bypasses Datadome perfectly because it impersonates Chrome's TLS fingerprint!
    r = requests.get(product_url, impersonate="chrome110")
    html = r.text
    
    # Extrair título
    title = ""
    title_match = re.search(r'<title>(.*?)</title>', html)
    if title_match:
        title = title_match.group(1).replace(" | Shopee Brasil", "").strip()

    # Procurar por URLs de MP4 dentro do HTML (a Shopee envia o vídeo inicial no código-fonte)
    mp4_urls = re.findall(r'https?:\/\/[^\s"\'<>]*\.mp4[^\s"\'<>]*', html)
    
    video_url = None
    if mp4_urls:
        # Pega a primeira URL válida que termina em mp4
        for url in mp4_urls:
            clean_url = url.replace('\\', '')
            if ".mp4" in clean_url:
                video_url = clean_url
                break
                
    if not video_url:
        print("[Shopee CFFI] Nenhum vídeo encontrado.")
        return {"titulo": title, "video_url": None, "media": []}
        
    print(f"[Shopee CFFI] Vídeo Encontrado! URL: {video_url}")
    
    # Fazer download do vídeo e subir no MinIO
    os.makedirs("temp_uploads", exist_ok=True)
    filename = f"temp_uploads/shopee_vid_{int(time.time())}.mp4"
    
    try:
        vid_res = std_requests.get(video_url, stream=True, timeout=60)
        with open(filename, "wb") as f:
            for chunk in vid_res.iter_content(chunk_size=65536):
                f.write(chunk)
                
        minio_url = upload_to_minio(filename, f"shopee/videos/{os.path.basename(filename)}", "video/mp4")
        os.remove(filename)
        
        return {
            "titulo": title,
            "linksMedia": [{"tipo": "VIDEO", "url": minio_url}]
        }
    except Exception as e:
        print(f"[Shopee CFFI] Erro no download/upload: {e}")
        if os.path.exists(filename):
            os.remove(filename)
        return {"titulo": title, "error": str(e), "linksMedia": []}

if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "https://shopee.com.br/2026-Smartwatch-T10-Ultra-3-Nova-S%C3%A9rie-10-SmartWatch-2.09-Inch-HD-49mm-Bluetooth-Com-Calculadora-i.952449950.22797032581"
    res = scrape_shopee_video(url)
    print(json.dumps(res, indent=2))
