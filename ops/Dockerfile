FROM python:3.11-slim

COPY ops/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/app

COPY ops/app.py /usr/app/app.py
COPY shared/db.py /usr/app/db.py
COPY ops/routers/ /usr/app/routers/
COPY ops/models/ /usr/app/models/
COPY ops/templates/ /usr/app/templates/
COPY ops/__init__.py /usr/app/__init__.py

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8060"]
