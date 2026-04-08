FROM python:3.13-slim

# Install dbt-postgres and uvicorn
RUN pip install dbt-postgres==1.8.2 uvicorn --no-cache-dir

WORKDIR /app

# Install additional dependencies
COPY dbt_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project for proper package structure
COPY . .

ENV DBT_PROFILES_DIR=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8080
ENTRYPOINT []
CMD ["uvicorn", "dbt_runner.app:app", "--host", "0.0.0.0", "--port", "8080"]
