FROM python:3.11-slim

WORKDIR /app

COPY container_health/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Only this package, unlike the other services' `COPY . .`. It imports nothing
# from shared/ and reads no repo file at runtime, and keeping the image that
# narrow is part of the point: the container holding the Docker grant should
# carry as little else as possible.
COPY container_health/ ./container_health/

CMD ["uvicorn", "container_health.app:app", "--host", "0.0.0.0", "--port", "9110"]
