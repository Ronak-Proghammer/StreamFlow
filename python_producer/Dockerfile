# ============================================================
# Python Producer Dockerfile
# ============================================================
# Using slim variant = smaller image, fewer vulnerabilities
# alpine is even smaller but can have compatibility issues
# with some Python packages (uses musl libc instead of glibc)
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run as non-root user
RUN useradd -m producer && chown -R producer:producer /app
USER producer

CMD ["python", "-u", "producer.py"]
# -u flag = unbuffered output, so logs appear immediately