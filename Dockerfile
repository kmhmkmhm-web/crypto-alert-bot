FROM python:3.11-slim

# Çalışma dizinini oluştur
WORKDIR /app

# Sistem bağımlılıklarını yükle
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Requirements'ı kopyala ve yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyala
COPY main.py .
COPY market_cap_cache.py .

# /tmp dizinini oluştur (cache için)
RUN mkdir -p /tmp

# Uygulamayı çalıştır
CMD ["python", "-u", "main.py"]
