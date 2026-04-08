FROM python:3.11-slim

WORKDIR /app

# Copy requirements first
COPY scraper/requirements.txt .

# Install deps
RUN pip install --no-cache-dir -r requirements.txt
RUN patchright install chromium --with-deps

# Copy the whole project (so processors/, db.py, etc. are included)
COPY . .

# Run the API on all interfaces so host + n8n can reach it
CMD ["uvicorn", "scraper.app:app", "--host", "0.0.0.0", "--port", "8000"]
