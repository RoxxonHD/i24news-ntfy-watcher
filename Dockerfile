FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DB_PATH=/data/seen_news.db

WORKDIR /app

# Crear usuario no-root y directorio persistente para SQLite
RUN groupadd -r watcher && useradd -r -g watcher watcher \
    && mkdir -p /data && chown -R watcher:watcher /data /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY watcher.py .
COPY .env.example .

USER watcher

VOLUME ["/data"]

CMD ["python", "watcher.py"]
