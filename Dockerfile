# Usa Python 3.10 leve
FROM python:3.10-slim

# 1. Instala dependências do Sistema Operacional (FFMPEG, ImageMagick, Ghostscript, Git)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    imagemagick \
    ghostscript \
    git \
    fonts-liberation \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Fix para o MoviePy no Debian/Ubuntu: liberar política de segurança do ImageMagick
RUN sed -i '/<policy domain="path" rights="none" pattern="@\*"/d' /etc/ImageMagick*/policy.xml

# Define pasta de trabalho
WORKDIR /app

# 3. Copia e instala as libs do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copia todo o código da pasta worker para dentro do container
COPY . .

# 5. Cria as pastas de output para não dar erro de permissão no Motor de Vídeo
RUN mkdir -p temp_uploads temp_outputs

# 6. Comando para iniciar o servidor (Porta 80)
# Ajustado de main:app para video:app que é o nome do seu arquivo real!
CMD ["uvicorn", "video:app", "--host", "0.0.0.0", "--port", "80"]
