FROM python:3.11-slim

WORKDIR /app

COPY ops/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "ops.app:app", "--host", "0.0.0.0", "--port", "8060"]
