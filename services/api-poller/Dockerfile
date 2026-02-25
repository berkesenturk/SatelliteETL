FROM python:3.11-slim

LABEL maintainer="berkesenturk11@gmail.com"
LABEL description="SEVIRI Pipeline API Poller Service"

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


COPY . .

RUN mkdir -p /logs

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1


EXPOSE 8000

CMD ["python", "app.py"]

