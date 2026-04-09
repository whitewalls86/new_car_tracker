FROM python:3.11-slim

WORKDIR /app

# Copy requirements first
COPY scraper/requirements.txt .

# Install deps
RUN pip install --no-cache-dir -r requirements.txt
RUN patchright install chromium --with-deps

# Copy the whole project (so processors/, etc. are included)
COPY . .

# Make scraper/db.py available at top level for "from db import" statements
RUN cp scraper/db.py db.py

# Run the API on all interfaces so host + n8n can reach it
CMD ["uvicorn", "scraper.app:app", "--host", "0.0.0.0", "--port", "8000"]
