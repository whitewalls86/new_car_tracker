FROM python:3.11-slim

WORKDIR /app

# Copy requirements first
COPY scraper/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the whole project (so processors/, etc. are included)
COPY . .

# Plan 162 Stage 8 removed a `RUN cp scraper/db.py db.py` here. app.py was the
# only module importing it bare, and the copy made scraper/db.py importable
# under two names -- so the module carried two `_pool` globals, and the suite
# could not `import scraper.app` at all outside the tests/scraper/ conftest
# that put scraper/ on sys.path. app.py now imports `scraper.db` like every
# other module in the package, which is what lets Layer 4 reach this app.

CMD ["uvicorn", "scraper.app:app", "--host", "0.0.0.0", "--port", "8000"]
