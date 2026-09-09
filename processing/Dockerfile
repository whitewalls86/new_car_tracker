FROM python:3.11-slim

WORKDIR /app

COPY processing/requirements.txt constraints.txt ./
RUN pip install --no-cache-dir -c constraints.txt -r requirements.txt

COPY . .

CMD ["uvicorn", "processing.app:app", "--host", "0.0.0.0", "--port", "8070"]
