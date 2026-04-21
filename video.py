import shutil
import os
import platform

# PRE-CONFIGURAÇÃO DO MOVIEPY (Precisa ser antes do import)
if platform.system() == "Windows":
    os.environ["IMAGEMAGICK_BINARY"] = r"C:\Program Files\ImageMagick-7.1.2-Q16\magick.exe"

import json
import math
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pdfplumber
import edge_tts
import whisper

# Imports do MoviePy (agora vai ler a variável acima com sucesso)
from moviepy.editor import *
from moviepy.video.tools.subtitles import SubtitlesClip
from moviepy.config import change_settings
import PIL.Image
from moviepy.config import change_settings
import PIL.Image

# --- PATCH PARA PILLOW 10.0.0+ (Erro 'ANTIALIAS') ---
if not hasattr(PIL.Image, 'ANTIALIAS'):
    PIL.Image.ANTIALIAS = PIL.Image.LANCZOS

# --- SETUP DA API ---
app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuração de caminhos (Temp folders)
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
    # speed format for edge_tts is like "+0%", "-10%"
    communicate = edge_tts.Communicate(text, voice, rate=speed)
    await communicate.save(output_file)

def generate_subtitles(audio_path):
    model = whisper.load_model("base")
    result = model.transcribe(audio_path, language="pt")
    subs = []
    for segment in result["segments"]:
        subs.append(((segment["start"], segment["end"]), segment["text"]))
    return subs

def create_video_logic(audio_path, subtitles_data, output_file, 
                       images: List[str] = [], image_durations: List[float] = [], 
                       bg_color: str = "#000000",
                       subtitle_config: dict = None,
                       video_format: str = "landscape", # landscape (16:9) or portrait (9:16)
                       image_config: dict = None): # zoom, pan_x, pan_y
    
    # 1. Define Resolution based on Format
    if video_format == "portrait":
        W, H = 1080, 1920
    else:
        W, H = 1920, 1080
        
    # Default configs
    if not subtitle_config: subtitle_config = {}
    if not image_config: image_config = {}
    
    # Subtitle defaults
    sub_conf = {
        "fontsize": 40 if video_format == "landscape" else 50, # Bigger for portrait
        "color": "white",
        "stroke_color": "black",
        "stroke_width": 2,
        "font": "Arial",
        "bg_color": "transparent",
        "position_y": "bottom" # bottom, center, top
    }
    sub_conf.update(subtitle_config)
    
    # Image defaults
    img_conf = {
        "zoom": 1.0,
        "pan_x": 0, # Pixels to shift X
        "pan_y": 0  # Pixels to shift Y
    }
    img_conf.update(image_config)

    # 2. Generator for Subtitles
    # Calculate vertical position
    # MoviePy set_position accepts ('center', 'bottom'), or (x, y)
    
    # We will position the SubtitlesClip later, here we define the TextClip properties
    def generator(txt):
        return TextClip(txt, 
                        font=sub_conf["font"], 
                        fontsize=int(sub_conf["fontsize"]), 
                        color=sub_conf["color"], 
                        method='caption', 
                        size=(W * 0.9, None), # 90% width, dynamic height
                        bg_color=sub_conf.get("bg_color"),
                        stroke_color=sub_conf["stroke_color"], 
                        stroke_width=float(sub_conf["stroke_width"]))

    audio = AudioFileClip(audio_path)
    total_duration = audio.duration
    
    # 3. Background Logic (Strict Exclusive)
    final_bg = None
    
    if images and len(images) > 0:
        clips = []
        for i, img_path in enumerate(images):
            try:
                # Duration
                percentage = image_durations[i] if i < len(image_durations) else (100 / len(images))
                duration = (percentage / 100) * total_duration
                
                # Image Processing - ROBUST METHOD
                # 1. Open with PIL
                # 2. Convert to RGB (Force 3 channels)
                # 3. Save to a temporary JPG file
                # 4. Load that JPG with MoviePy
                
                processed_path = img_path + f"_proc_{i}.jpg"
                
                with PIL.Image.open(img_path) as im:
                    im = im.convert("RGB") # Fixes the shape mismatch (grayscale vs rgb)
                    
                    # Optional: Resize large images to avoid memory issues, keeping aspect ratio
                    # processing 4k images for 1080p output is wasteful
                    # but let's just save for now to be safe
                    im.save(processed_path, quality=95)
                
                # Create Clip from the PROCESSED file
                img_clip = ImageClip(processed_path).set_duration(duration)
                
                # RESIZE & CROP LOGIC (Cover/Fill)
                # First, resize to cover the screen dimensions
                # Calculate ratio
                img_w, img_h = img_clip.size
                ratio_img = img_w / img_h
                ratio_screen = W / H
                
                # If image is wider (relative to screen), fit height and crop width
                if ratio_img > ratio_screen:
                     img_clip = img_clip.resize(height=H)
                else: 
                     img_clip = img_clip.resize(width=W)
                     
                # Apply Zoom
                if img_conf["zoom"] != 1.0:
                    # Apply static zoom
                    current_w, current_h = img_clip.size
                    img_clip = img_clip.resize(newsize=(current_w * img_conf["zoom"], current_h * img_conf["zoom"]))

                # Center and Apply Pan
                cw, ch = img_clip.size
                
                # Center coordinates based on Resize logic
                x_center = (W - cw) / 2
                y_center = (H - ch) / 2
                
                pos_x = x_center + float(img_conf["pan_x"])
                pos_y = y_center + float(img_conf["pan_y"])
                
                # Background buffer to avoid transparency issues
                rgb_bg = tuple(int(bg_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))

                bg_buffer = ColorClip(size=(W, H), color=rgb_bg, duration=duration)
                
                combined = CompositeVideoClip([
                    bg_buffer, 
                    img_clip.set_position((pos_x, pos_y))
                ], size=(W,H))
                
                clips.append(combined)
            except Exception as e:
                print(f"Error processing image {i}: {e}")
                # Fallback to color to prevent pipeline crash
                perc = image_durations[i] if i < len(image_durations) else (100/len(images))
                dur = (perc / 100) * total_duration
                clips.append(ColorClip(size=(W, H), color=bg_color, duration=dur))

        final_bg = concatenate_videoclips(clips).to_RGB()
        
        # Duration Fix
        if final_bg.duration < total_duration:
             final_bg = final_bg.set_duration(total_duration)
        elif final_bg.duration > total_duration:
             final_bg = final_bg.subclip(0, total_duration)

    else:
        # PURE COLOR MODE
        video_color = tuple(int(bg_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
        final_bg = ColorClip(size=(W, H), color=video_color, duration=total_duration)

    # 4. Composite Subtitles
    subtitles = SubtitlesClip(subtitles_data, generator)

    subtitles = subtitles.set_duration(total_duration).to_RGB()
    
    # Subtitle Positioning
    # User might choose: bottom, center, top
    # Or specific margin. Let's map simple presets.
    
    sub_pos = ('center', 'center') # Default
    if sub_conf["position_y"] == "bottom":
        sub_pos = ('center', 0.8 if video_format == "landscape" else 0.75) # 80% down
    elif sub_conf["position_y"] == "top":
        sub_pos = ('center', 0.1) # 10% down
    elif sub_conf["position_y"] == "center":
        sub_pos = ('center', 'center')

    final_bg = final_bg.to_RGB()
        
    final = CompositeVideoClip([final_bg, subtitles.set_position(sub_pos, relative=True)], size=(W,H))
    final.audio = audio

    final = final.set_mask(None)
    
    final.write_videofile(output_file, fps=24, codec="libx264", audio_codec="aac", preset="ultrafast")

    
# --- ENDPOINTS ---

@app.post("/extrair-texto")
async def extrair_texto(file: UploadFile = File(...)):
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

@app.post("/simular-audio")
async def simular_audio(
    text: str = Form(...),
    voice: str = Form("pt-BR-AntonioNeural"),
    speed: str = Form("+0%")
):
    if len(text) > 150: 
        raise HTTPException(status_code=400, detail="Texto muito longo para simulação (max 100 caracteres)")
        
    try:
        temp_audio = os.path.join(OUTPUT_DIR, f"sim_{os.urandom(4).hex()}.mp3")
        await generate_audio(text, temp_audio, voice, speed)
        return FileResponse(temp_audio, media_type="audio/mpeg", filename="preview.mp3")
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
    video_format: str = Form("landscape"), # landscape, portrait
    image_config: str = Form("{}") # zoom, pan params
):
    # 1. Obter Texto
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
        print(">>> 1. Gerando áudio...")
        await generate_audio(working_text, audio_path, voice, speed)

        print(">>> 2. Gerando legendas...")
        subs = generate_subtitles(audio_path)

        print(f">>> 3. Renderizando vídeo ({video_format})...")
        create_video_logic(
            audio_path, 
            subs, 
            video_path, 
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
