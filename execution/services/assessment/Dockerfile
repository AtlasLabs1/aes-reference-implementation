FROM python:3.12-slim

WORKDIR /app

COPY execution/services/assessment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY execution/common ./common
COPY execution/services/assessment/app ./app
COPY dkms ./dkms

ENV PYTHONPATH=/app/common
ENV AES_DKM_PATH=/app/dkms/water-system

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
