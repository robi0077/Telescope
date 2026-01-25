
# 1. Base Image (Official Python)
FROM python:3.10-slim

# 2. System Dependencies (ffmpeg, git for source, gcc for building av)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    pkg-config \
    git \
    && rm -rf /var/lib/apt/lists/*

# 3. Work Directory
WORKDIR /app

# 4. Install Python Dependencies
# Copy requirements first to leverage Docker Cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy Source Code
COPY src/ src/
COPY setup.py .

# 6. Install Telescope (Editable or Standard)
RUN pip install -e .

# 7. Environment Variables (Defaults, override in Compose)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UPLOAD_DIR=/app/temp_uploads

# 8. Create Upload Dir
RUN mkdir -p /app/temp_uploads

# 9. Entrypoint (Default to API, override command for Worker)
EXPOSE 8000
CMD ["uvicorn", "telescope.server:app", "--host", "0.0.0.0", "--port", "8000"]
