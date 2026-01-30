FROM python:3.11-slim

WORKDIR /app

# Copy the whole project (so processors/, parsers/, etc. are included)
COPY .. .

# Install deps
RUN pip install --no-cache-dir -r requirements.txt

# Run the API on all interfaces so host + n8n can reach it
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
