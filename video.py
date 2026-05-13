import shutil
import os
import platform
import json
import math
import time
import requests
import os
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import pdfplumber
import edge_tts
import whisper
import PIL.Image
import PIL.ImageDraw
import numpy as np

# PRE-CONFIGURAÇÃO DO MOVIEPY
if platform.system() == "Windows":
    os.environ["IMAGEMAGICK_BINARY"] = r"C:\Program Files\ImageMagick-7.1.2-Q16\magick.exe"

from moviepy.editor import *
from moviepy.video.tools.subtitles import SubtitlesClip
from moviepy.config import change_settings
from shopee_scraper_service import scrape_shopee_product, scrape_shopee_raw, upload_to_minio

# --- PATCH PARA PILLOW 10.0.0+ ---
if not hasattr(PIL.Image, 'ANTIALIAS'):
    PIL.Image.ANTIALIAS = PIL.Image.LANCZOS

# --- SETUP DA API ---
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "temp_uploads"
OUTPUT_DIR = "temp_outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

if platform.system() == "Windows":
    change_settings({"IMAGEMAGICK_BINARY": r"magick"})

# --- FUNÇÕES CORE ---

def extract_text(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t: text += t + " "
    return text.replace('\n', ' ').strip()

async def generate_audio(text, output_file, voice="pt-BR-AntonioNeural", speed="+0%"):
    communicate = edge_tts.Communicate(text, voice, rate=speed)
    await communicate.save(output_file)

def generate_subtitles(audio_path):
    model = whisper.load_model("base")
    result = model.transcribe(audio_path, language="pt")
    subs = []
    for segment in result["segments"]:
        subs.append(((segment["start"], segment["end"]), segment["text"]))
    return subs

def generate_word_timestamps(audio_path):
    model = whisper.load_model("base")
    result = model.transcribe(audio_path, language="pt", word_timestamps=True)
    words = []
    for segment in result["segments"]:
        for word in segment.get("words", []):
            words.append({
                "word": word["word"],
                "start": word["start"],
                "end": word["end"],
                "probability": word.get("probability")
            })
    return words

def create_video_logic(audio_path, subtitles_data, output_file, 
                       images: List[str] = [], image_durations: List[float] = [], 
                       bg_color: str = "#000000",
                       subtitle_config: dict = None,
                       video_format: str = "landscape", 
                       image_config: dict = None):
    
    if video_format == "portrait":
        W, H = 1080, 1920
    else:
        W, H = 1920, 1080
        
    if not subtitle_config: subtitle_config = {}
    if not image_config: image_config = {}
    
    sub_conf = {
        "fontsize": 40 if video_format == "landscape" else 50,
        "color": "white",
        "stroke_color": "black",
        "stroke_width": 2,
        "font": "Arial",
        "bg_color": "transparent",
        "position_y": "bottom"
    }
    sub_conf.update(subtitle_config)
    
    img_conf = {
        "zoom": 1.0,
        "pan_x": 0,
        "pan_y": 0
    }
    img_conf.update(image_config)

    def generator(txt):
        return TextClip(txt, 
                        font=sub_conf["font"], 
                        fontsize=int(sub_conf["fontsize"]), 
                        color=sub_conf["color"], 
                        method='caption', 
                        size=(W * 0.9, None),
                        bg_color=sub_conf.get("bg_color"),
                        stroke_color=sub_conf["stroke_color"], 
                        stroke_width=float(sub_conf["stroke_width"]))

    audio = AudioFileClip(audio_path)
    total_duration = audio.duration
    
    final_bg = None
    if images and len(images) > 0:
        clips = []
        for i, img_path in enumerate(images):
            try:
                percentage = image_durations[i] if i < len(image_durations) else (100 / len(images))
                duration = (percentage / 100) * total_duration
                
                video_extensions = {'.mp4', '.mov', '.avi', '.mkv', '.webm'}
                img_ext = os.path.splitext(img_path)[1].lower()
                
                if img_ext in video_extensions:
                    raw_clip = VideoFileClip(img_path).without_audio()
                    if raw_clip.duration < duration:
                        raw_clip = raw_clip.loop(duration=duration)
                    else:
                        raw_clip = raw_clip.subclip(0, duration)
                    
                    cv_w, cv_h = raw_clip.size
                    ratio_vid = cv_w / cv_h
                    ratio_screen = W / H
                    if ratio_vid > ratio_screen:
                        raw_clip = raw_clip.resize(height=H)
                    else:
                        raw_clip = raw_clip.resize(width=W)
                    img_clip = raw_clip
                else:
                    processed_path = img_path + f"_proc_{i}.jpg"
                    with PIL.Image.open(img_path) as im:
                        im = im.convert("RGB")
                        im.save(processed_path, quality=95)
                    
                    img_clip = ImageClip(processed_path).set_duration(duration)
                    img_w, img_h = img_clip.size
                    ratio_img = img_w / img_h
                    ratio_screen = W / H
                    if ratio_img > ratio_screen:
                         img_clip = img_clip.resize(height=H)
                    else: 
                         img_clip = img_clip.resize(width=W)
                
                if img_conf["zoom"] != 1.0:
                    current_w, current_h = img_clip.size
                    img_clip = img_clip.resize(newsize=(current_w * img_conf["zoom"], current_h * img_conf["zoom"]))

                cw, ch = img_clip.size
                x_center = (W - cw) / 2
                y_center = (H - ch) / 2
                pos_x = x_center + float(img_conf["pan_x"])
                pos_y = y_center + float(img_conf["pan_y"])
                
                rgb_bg = tuple(int(bg_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
                bg_buffer = ColorClip(size=(W, H), color=rgb_bg, duration=duration)
                combined = CompositeVideoClip([bg_buffer, img_clip.set_position((pos_x, pos_y))], size=(W,H))
                clips.append(combined)
            except Exception as e:
                print(f"Error processing media {i}: {e}")
                perc = image_durations[i] if i < len(image_durations) else (100/len(images))
                dur = (perc / 100) * total_duration
                rgb_bg = tuple(int(bg_color.lstrip('#')[j:j+2], 16) for j in (0, 2, 4))
                clips.append(ColorClip(size=(W, H), color=rgb_bg, duration=dur))

        final_bg = concatenate_videoclips(clips).to_RGB()
        if final_bg.duration < total_duration:
             final_bg = final_bg.set_duration(total_duration)
        elif final_bg.duration > total_duration:
             final_bg = final_bg.subclip(0, total_duration)
    else:
        video_color = tuple(int(bg_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
        final_bg = ColorClip(size=(W, H), color=video_color, duration=total_duration)

    subtitles = SubtitlesClip(subtitles_data, generator)
    subtitles = subtitles.set_duration(total_duration).to_RGB()
    
    sub_pos = ('center', 'center')
    if sub_conf["position_y"] == "bottom":
        sub_pos = ('center', 0.8 if video_format == "landscape" else 0.75)
    elif sub_conf["position_y"] == "top":
        sub_pos = ('center', 0.1)
    elif sub_conf["position_y"] == "center":
        sub_pos = ('center', 'center')

    final_bg = final_bg.to_RGB()
    final = CompositeVideoClip([final_bg, subtitles.set_position(sub_pos, relative=True)], size=(W,H))
    final.audio = audio
    final = final.set_mask(None)
    final.write_videofile(output_file, fps=24, codec="libx264", audio_codec="aac", preset="ultrafast")


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES PARA GERAÇÃO DE VÍDEO TIKTOK (COLETA SHOPEE)
# ─────────────────────────────────────────────────────────────────────────────

def make_rounded_mask(size: tuple, radius: int) -> ImageClip:
    """
    Cria uma máscara retangular com cantos arredondados para o PiP.
    Retorna um ImageClip em escala de cinza (0-1) para uso como máscara.
    """
    img = PIL.Image.new("L", size, 0)
    draw = PIL.ImageDraw.Draw(img)
    draw.rounded_rectangle([(0, 0), (size[0] - 1, size[1] - 1)], radius=radius, fill=255)
    mask_array = np.array(img).astype(float) / 255.0
    return ImageClip(mask_array, ismask=True)


def resolve_media_url(item) -> str:
    """
    Aceita string simples ou objeto vindo do banco/Next.
    Prioriza urlMinio, depois url.
    """
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        return str(item.get("urlMinio") or item.get("url") or "").strip()
    return ""

def fit_cover(clip: VideoClip, W: int, H: int) -> VideoClip:
    """
    Redimensiona o clipe para cobrir WxH sem distorcer (crop central).
    """
    try:
        cw, ch = clip.size
        if not cw or not ch:
            return clip
        scale = max(W / cw, H / ch)
        new_w = int(cw * scale)
        new_h = int(ch * scale)
        resized = clip.resize((new_w, new_h))
        x = (new_w - W) // 2
        y = (new_h - H) // 2
        return resized.crop(x1=x, y1=y, x2=x + W, y2=y + H)
    except Exception:
        return clip

def download_to_file(url: str, target_path: str, timeout: int = 120) -> None:
    with requests.get(url, timeout=timeout, stream=True) as resp:
        resp.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def build_main_background(media_paths: list, total_duration: float, W: int, H: int) -> VideoClip:
    """
    Monta o vídeo de fundo com as mídias do produto.
    Ordem: vídeos primeiro, depois imagens.
    TODOS os clipes ficam SEM ÁUDIO (áudio do produto é removido).
    """
    # Separar e ordenar: vídeos primeiro, imagens depois
    video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v'}
    videos = [p for p in media_paths if os.path.splitext(p)[1].lower() in video_exts]
    images = [p for p in media_paths if os.path.splitext(p)[1].lower() not in video_exts]
    ordered = videos + images  # vídeos primeiro, imagens depois

    if not ordered:
        return ColorClip(size=(W, H), color=(15, 15, 15), duration=total_duration)

    # Duração de cada clip proporcional
    dur_each = total_duration / len(ordered)

    clips = []
    for i, path in enumerate(ordered):
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext in video_exts:
                # Vídeo do produto: remover áudio completamente
                clip = VideoFileClip(path).without_audio()
                # Loop se o vídeo for mais curto que a duração alocada
                if clip.duration < dur_each:
                    clip = clip.loop(duration=dur_each)
                else:
                    clip = clip.subclip(0, dur_each)
            else:
                # Imagem: processar e criar clip estático
                processed = path + f"_bg_{i}.jpg"
                with PIL.Image.open(path) as im:
                    im = im.convert("RGB")
                    im.save(processed, quality=95)
                clip = ImageClip(processed).set_duration(dur_each)

            # Redimensionar para cobrir o frame 9:16 (cover, sem deformar)
            cw, ch = clip.size
            scale = max(W / cw, H / ch)
            new_w = int(cw * scale)
            new_h = int(ch * scale)
            clip = clip.resize((new_w, new_h))
            # Centralizar no frame
            x = (W - new_w) // 2
            y = (H - new_h) // 2

            bg = ColorClip(size=(W, H), color=(0, 0, 0), duration=dur_each)
            composed = CompositeVideoClip(
                [bg, clip.set_position((x, y))],
                size=(W, H)
            )
            clips.append(composed)
        except Exception as e:
            print(f"[TikTok] Erro ao processar mídia {i} ({path}): {e}")
            clips.append(ColorClip(size=(W, H), color=(20, 20, 20), duration=dur_each))

    main = concatenate_videoclips(clips)
    # Ajuste fino de duração
    if main.duration < total_duration:
        main = main.set_duration(total_duration)
    elif main.duration > total_duration:
        main = main.subclip(0, total_duration)
    return main.to_RGB()


def build_pip_clip(reaction_path: str, pip_w: int, pip_h: int, pip_radius: int) -> VideoClip:
    """
    Carrega o vídeo de reação, redimensiona para o quadradinho PiP e
    aplica máscara com cantos arredondados.
    O áudio é mantido aqui — será o áudio final do vídeo.
    """
    pip = VideoFileClip(reaction_path)
    # Redimensionar mantendo proporção, preenchendo o quadrado PiP
    pw, ph = pip.size
    scale = max(pip_w / pw, pip_h / ph)
    new_w = int(pw * scale)
    new_h = int(ph * scale)
    pip = pip.resize((new_w, new_h))
    # Crop centralizado no tamanho pip_w x pip_h
    x_crop = (new_w - pip_w) // 2
    y_crop = (new_h - pip_h) // 2
    pip = pip.crop(x1=x_crop, y1=y_crop, x2=x_crop + pip_w, y2=y_crop + pip_h)
    # Aplicar máscara arredondada
    mask = make_rounded_mask((pip_w, pip_h), pip_radius)
    pip = pip.set_mask(mask)
    return pip


def create_tiktok_product_video(
    media_paths: list,
    reaction_path: str,
    output_path: str,
    pip_fraction: float = 0.30,
    pip_margin: int = 30,
    pip_radius: int = 20,
):
    """
    Compõe o vídeo final TikTok (1080×1920):
      - Fundo: mídias do produto (sem áudio) — vídeos primeiro, depois imagens
      - Overlay PiP: vídeo de reação no canto inferior direito (com áudio)

    O áudio do vídeo de reação é o único áudio do vídeo final.
    """
    W, H = 1080, 1920

    # Carregar vídeo de reação para obter duração e áudio
    reaction_clip = VideoFileClip(reaction_path)
    total_duration = reaction_clip.duration
    reaction_audio = reaction_clip.audio  # será o áudio final

    # Dimensões do PiP (~30% da largura)
    pip_w = int(W * pip_fraction)
    pip_h = pip_w  # quadrado

    # Montar fundo com mídias do produto (sem áudio)
    main_bg = build_main_background(media_paths, total_duration, W, H)

    # Montar PiP (sem áudio por enquanto — adicionamos depois)
    pip_clip = build_pip_clip(reaction_path, pip_w, pip_h, pip_radius)
    pip_clip_no_audio = pip_clip.without_audio()

    # Posição PiP: canto inferior direito
    pip_x = W - pip_w - pip_margin
    pip_y = H - pip_h - pip_margin

    # Composição final
    final = CompositeVideoClip(
        [
            main_bg,
            pip_clip_no_audio.set_position((pip_x, pip_y)),
        ],
        size=(W, H),
    )

    # Definir duração e áudio (somente do vídeo de reação)
    final = final.set_duration(total_duration)
    if reaction_audio:
        final = final.set_audio(reaction_audio)

    print(f"[TikTok] Exportando vídeo {W}x{H}, {total_duration:.1f}s → {output_path}")
    final.write_videofile(
        output_path,
        fps=30,
        codec="libx264",
        audio_codec="aac",
        preset="ultrafast",
        threads=4,
    )
    print(f"[TikTok] Vídeo gerado com sucesso: {output_path}")


# --- ENDPOINTS ---

@app.post("/transcrever-palavras")
async def transcrever_palavras(file: UploadFile = File(...)):
    try:
        audio_path = os.path.join(UPLOAD_DIR, f"transc_{os.urandom(4).hex()}_{file.filename}")
        with open(audio_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        words = generate_word_timestamps(audio_path)
        return JSONResponse({"words": words})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/extrair-texto")
async def extrair_texto_endpoint(file: UploadFile = File(...)):
    try:
        pdf_path = os.path.join(UPLOAD_DIR, f"extract_{file.filename}")
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        text = extract_text(pdf_path)
        estimated_seconds = len(text) / 15 
        return JSONResponse({
            "text": text,
            "estimated_duration_seconds": estimated_seconds,
            "char_count": len(text)
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/gerar-audio")
async def gerar_audio_endpoint(
    text: str = Form(...),
    voice: str = Form("pt-BR-AntonioNeural"),
    speed: str = Form("+0%")
):
    try:
        base_name = f"audio_{os.urandom(4).hex()}"
        audio_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp3")
        await generate_audio(text, audio_path, voice, speed)
        return FileResponse(audio_path, media_type="audio/mpeg", filename=f"{base_name}.mp3")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/gerar-video")
async def gerar_video_endpoint(
    file: Optional[UploadFile] = File(None),
    text: Optional[str] = Form(None),
    images: List[UploadFile] = File(None),
    image_durations: str = Form("[]"), 
    background_color: str = Form("#000000"),
    voice: str = Form("pt-BR-AntonioNeural"),
    speed: str = Form("+0%"),
    subtitle_config: str = Form("{}"),
    video_format: str = Form("landscape"),
    image_config: str = Form("{}")
):
    working_text = ""
    if text:
        working_text = text
    elif file:
        pdf_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        working_text = extract_text(pdf_path)
    else:
        return JSONResponse({"error": "Forneça um arquivo PDF ou texto."}, status_code=400)
    
    base_name = f"video_{os.urandom(4).hex()}"
    audio_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp3")
    video_path = os.path.join(OUTPUT_DIR, f"{base_name}.mp4")

    saved_images = []
    if images:
        for img in images:
            img_path = os.path.join(UPLOAD_DIR, f"img_{os.urandom(4).hex()}_{img.filename}")
            with open(img_path, "wb") as buffer:
                 shutil.copyfileobj(img.file, buffer)
            saved_images.append(img_path)
            
    try:
        durations_list = json.loads(image_durations)
        sub_conf = json.loads(subtitle_config)
        img_conf = json.loads(image_config)
    except:
        durations_list = []
        sub_conf = {}
        img_conf = {}

    try:
        await generate_audio(working_text, audio_path, voice, speed)
        subs = generate_subtitles(audio_path)
        create_video_logic(
            audio_path, subs, video_path, 
            images=saved_images, 
            image_durations=durations_list,
            bg_color=background_color,
            subtitle_config=sub_conf,
            video_format=video_format,
            image_config=img_conf
        )
        return FileResponse(video_path, media_type="video/mp4", filename=f"{base_name}.mp4")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/scraping-shopee")
async def scraping_shopee_endpoint(url: str = Form(...)):
    try:
        data = await scrape_shopee_product(url)
        return JSONResponse(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/scraping-shopee-raw")
async def scraping_shopee_raw_endpoint(url: str = Form(...)):
    """
    Returns raw video/image URLs found in the Shopee page (no MinIO upload).
    Designed to be called by the render-service which has its own MinIO credentials.
    """
    try:
        data = await scrape_shopee_raw(url)
        return JSONResponse(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/gerar-video-tiktok")
async def gerar_video_tiktok_endpoint(
    coleta_id: str = Form(...),
    media_urls: str = Form(...),          # JSON array de URLs do MinIO
    reaction_video: UploadFile = File(...),
    pip_fraction: float = Form(0.30),     # tamanho PiP: 30% da largura
    pip_margin: int = Form(30),           # margem do canto (px)
    pip_radius: int = Form(20),           # raio dos cantos arredondados (px)
    upload_mode: str = Form("worker"),
):
    """
    Gera um vídeo TikTok (1080×1920) combinando as mídias do produto Shopee com
    um vídeo de reação em PiP no canto inferior direito.

    Regra de áudio: o áudio do produto é SEMPRE removido.
    Apenas o áudio do vídeo de reação é mantido no resultado final.
    """
    uid = os.urandom(6).hex()
    started_at = time.time()
    work_dir = os.path.join(UPLOAD_DIR, f"tiktok_{coleta_id}_{uid}")
    os.makedirs(work_dir, exist_ok=True)

    downloaded_paths = []
    reaction_path = None
    output_path = os.path.join(OUTPUT_DIR, f"tiktok_{coleta_id}_{uid}.mp4")

    try:
        # 1. Salvar vídeo de reação enviado pelo usuário
        reaction_path = os.path.join(work_dir, f"reaction_{reaction_video.filename}")
        with open(reaction_path, "wb") as f:
            shutil.copyfileobj(reaction_video.file, f)
        print(f"[TikTok] Vídeo de reação salvo: {reaction_path}")

        # 2. Fazer download das mídias do produto (URLs do MinIO)
        urls = json.loads(media_urls)
        print(f"[TikTok] Fazendo download de {len(urls)} mídias do produto...")
        for i, item in enumerate(urls):
            url = resolve_media_url(item)
            if not url:
                continue
            try:
                resp = requests.get(url, timeout=60, stream=True)
                resp.raise_for_status()
                # Determinar extensão pela URL ou content-type
                ct = resp.headers.get("content-type", "")
                if "video" in ct or url.lower().endswith((".mp4", ".mov", ".webm")):
                    ext = ".mp4"
                else:
                    ext = ".jpg"
                local_path = os.path.join(work_dir, f"media_{i}{ext}")
                with open(local_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                downloaded_paths.append(local_path)
                print(f"[TikTok] Mídia {i} baixada: {local_path}")
            except Exception as e:
                print(f"[TikTok] Erro ao baixar mídia {i} ({url}): {e}")

        if not downloaded_paths:
            return JSONResponse(
                {"error": "Nenhuma mídia do produto pôde ser baixada."},
                status_code=400,
            )

        # 3. Composição do vídeo TikTok
        print("[TikTok] Iniciando composição do vídeo...")
        create_tiktok_product_video(
            media_paths=downloaded_paths,
            reaction_path=reaction_path,
            output_path=output_path,
            pip_fraction=pip_fraction,
            pip_margin=pip_margin,
            pip_radius=pip_radius,
        )

        # 4. Upload do vídeo final para o MinIO
        if str(upload_mode).strip().lower() == "external":
            with open(output_path, "rb") as f:
                video_bytes = f.read()
            print(f"[TikTok] Retornando MP4 para upload externo: {len(video_bytes)} bytes")
            return Response(
                content=video_bytes,
                media_type="video/mp4",
                headers={"X-Coleta-Id": coleta_id, "X-TikTok-Uid": uid},
            )

        minio_key = f"shopee/videos-tiktok/tiktok_{coleta_id}_{uid}.mp4"
        print(f"[TikTok] Fazendo upload para MinIO: {minio_key}")
        final_url = upload_to_minio(output_path, minio_key, "video/mp4")
        print(f"[TikTok] URL final: {final_url}")

        return JSONResponse({
            "ok": True,
            "coleta_id": coleta_id,
            "videoUrl": final_url,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

    finally:
        # Limpeza dos arquivos temporários
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
            if output_path and os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass


@app.post("/merge-videos")
async def merge_videos_endpoint(
    coleta_id: str = Form(...),
    original_video_url: str = Form(...),
    copy_video_url: str = Form(...),
    upload_mode: str = Form("worker"),
):
    """
    Une (concatena) o vÃ­deo original do produto (sem Ã¡udio) com o vÃ­deo da copy (com Ã¡udio).
    SaÃ­da: MP4 vertical 1080x1920.
    """
    uid = os.urandom(6).hex()
    started_at = time.time()
    work_dir = os.path.join(UPLOAD_DIR, f"merge_{coleta_id}_{uid}")
    os.makedirs(work_dir, exist_ok=True)

    original_path = os.path.join(work_dir, "original.mp4")
    copy_path = os.path.join(work_dir, "copy.mp4")
    output_path = os.path.join(OUTPUT_DIR, f"merge_{coleta_id}_{uid}.mp4")

    try:
        orig_url = str(original_video_url or "").strip()
        copy_url = str(copy_video_url or "").strip()
        if not orig_url or not copy_url:
            return JSONResponse({"error": "original_video_url e copy_video_url sao obrigatorios."}, status_code=400)

        print("[Merge] Baixando vÃ­deos...", {"coleta_id": coleta_id, "uid": uid})
        download_to_file(orig_url, original_path, timeout=180)
        download_to_file(copy_url, copy_path, timeout=180)

        W, H = 1080, 1920
        original_clip = fit_cover(VideoFileClip(original_path).without_audio(), W, H)
        copy_clip = fit_cover(VideoFileClip(copy_path), W, H)

        final = concatenate_videoclips([original_clip, copy_clip], method="compose")
        print(f"[Merge] Exportando MP4 {W}x{H} -> {output_path}")
        final.write_videofile(
            output_path,
            fps=30,
            codec="libx264",
            audio_codec="aac",
            preset="ultrafast",
            threads=4,
        )

        elapsed = int((time.time() - started_at) * 1000)
        print("[Merge] OK", {"coleta_id": coleta_id, "elapsed_ms": elapsed})

        if str(upload_mode).strip().lower() == "external":
            with open(output_path, "rb") as f:
                video_bytes = f.read()
            return Response(
                content=video_bytes,
                media_type="video/mp4",
                headers={"X-Coleta-Id": coleta_id, "X-Merge-Uid": uid},
            )

        minio_key = f"shopee/videos-merged/merged_{coleta_id}_{uid}.mp4"
        final_url = upload_to_minio(output_path, minio_key, "video/mp4")
        return JSONResponse({"ok": True, "coleta_id": coleta_id, "videoUrl": final_url})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
            if os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass
