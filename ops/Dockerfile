FROM python:3.11-slim

COPY ops/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/app

COPY ops/app.py /usr/app/app.py

# Run the API on all interfaces so host + n8n can reach it
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8060"]
