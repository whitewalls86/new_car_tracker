FROM python:3.13-slim

WORKDIR /app

COPY archiver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "archiver.app:app", "--host", "0.0.0.0", "--port", "8001"]
