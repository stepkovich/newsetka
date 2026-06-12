FROM python:3.12-slim

WORKDIR /app

# Install system deps for pycryptodome
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY config.py .

# Create .env placeholder (user must provide real keys via volume mount)
RUN touch .env

CMD ["python", "bot.py"]
