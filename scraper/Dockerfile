FROM python:3.11-slim

WORKDIR /app

# Copy requirements first
COPY scraper/requirements.txt .

# Install deps + Xvfb for headed browser in headless Linux environment
RUN apt-get update && apt-get install -y --no-install-recommends xvfb && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
RUN patchright install chromium --with-deps

# Copy the whole project (so processors/, etc. are included)
COPY . .

# Make scraper/db.py available at top level for "from db import" statements
RUN cp scraper/db.py db.py

# Start a virtual display then launch the API
CMD ["sh", "-c", "Xvfb :99 -screen 0 1280x720x24 -nolisten tcp & DISPLAY=:99 uvicorn scraper.app:app --host 0.0.0.0 --port 8000"]
