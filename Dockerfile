# Imagem base com Python 3.10 + ffmpeg pré-instalado
# Muito menor que instalar ffmpeg do apt (evita 200+ pacotes extras)
FROM jrottenberg/ffmpeg:6.1-ubuntu2204 AS ffmpeg-base

FROM python:3.10-slim

# Copia apenas os binários do ffmpeg da imagem especializada
COPY --from=ffmpeg-base /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg-base /usr/local/bin/ffprobe /usr/local/bin/ffprobe
COPY --from=ffmpeg-base /usr/local/lib/ /usr/local/lib/

# Instala APENAS o essencial (sem ffmpeg via apt)
RUN apt-get update && apt-get install -y --no-install-recommends \
    imagemagick \
    ghostscript \
    fonts-liberation \
    libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Fix de segurança do ImageMagick (permite PDF/vídeo)
RUN sed -i 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml || true

WORKDIR /app

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip cache purge

# Copia código
COPY . .

# Pastas de output
RUN mkdir -p temp_uploads temp_outputs

# Roda os dois processos: video API na porta 80 + scraper daemon
# Usa um script de entrypoint para iniciar ambos
CMD uvicorn video:app --host 0.0.0.0 --port 80 & python scraper.py & wait
