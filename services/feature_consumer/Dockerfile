FROM python:3.11-slim

# Don't buffer Python output (see logs immediately)
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY state.py features.py db.py app.py ./

# Run the consumer
CMD ["python", "app.py"]
