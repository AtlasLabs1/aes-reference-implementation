FROM python:3.12-slim

WORKDIR /app

COPY execution/services/recommendation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY execution/common ./common
COPY execution/services/recommendation/app ./app
COPY dkms ./dkms

ENV PYTHONPATH=/app/common

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
