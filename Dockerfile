FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# RUN playwright install chromium
# RUN playwright install-deps chromium || true

COPY . .

# RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
# USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]