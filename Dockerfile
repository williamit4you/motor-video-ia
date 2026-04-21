# Usa Python 3.10 leve
FROM python:3.10-slim

# 1. Instala dependências do Sistema Operacional (FFMPEG, ImageMagick, Git)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    imagemagick \
    ghostscript \
    git \
    fonts-liberation \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Fix para ImageMagick: libera política de segurança
RUN sed -i '/<policy domain="path" rights="none" pattern="@\*"/d' /etc/ImageMagick*/policy.xml || true

# Define pasta de trabalho
WORKDIR /app

# 2. PyTorch CPU-only primeiro (evita baixar 2.5GB da versão CUDA)
COPY requirements.txt .
RUN pip install --no-cache-dir torch torchaudio --index-url https://download.pytorch.org/whl/cpu

# 3. Restante das libs do Python
RUN pip install --no-cache-dir -r requirements.txt \
    && pip cache purge

# 4. Copia o código
COPY . .

# 5. Cria pastas de output
RUN mkdir -p temp_uploads temp_outputs

# 6. Inicia o servidor de vídeo (porta 80) + aguarda 10s + inicia o scraper daemon
CMD uvicorn video:app --host 0.0.0.0 --port 80 & sleep 10 && python scraper.py