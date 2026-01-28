FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose ports
# 8001 - API
# 9094 - Prometheus metrics
EXPOSE 8001 9094

# Run the application
CMD ["python", "app.py"]
