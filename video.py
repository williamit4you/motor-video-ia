import shutil
import os
import platform
import json
import math
import os
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pdfplumber
import edge_tts
import whisper
import PIL.Image

# PRE-CONFIGURAÇÃO DO MOVIEPY
if platform.system() == "Windows":
    os.environ["IMAGEMAGICK_BINARY"] = r"C:\Program Files\ImageMagick-7.1.2-Q16\magick.exe"

from moviepy.editor import *
from moviepy.video.tools.subtitles import SubtitlesClip
from moviepy.config import change_settings

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
