FROM python:3.11-slim

# Don't buffer Python output
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY config.py data_loader.py evaluate.py train.py ./

# Default command: run training
CMD ["python", "train.py"]
