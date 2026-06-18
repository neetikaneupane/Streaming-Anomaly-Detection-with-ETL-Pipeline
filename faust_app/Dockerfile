FROM python:3.11-slim

# RocksDB needs build tools + librocksdb-dev on Linux
RUN apt-get update && apt-get install -y \
    build-essential \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "app.py", "worker", "-l", "info"]