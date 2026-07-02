FROM python:3.12-slim

WORKDIR /app

COPY execution/services/evidence/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY execution/common ./common
COPY execution/services/evidence/app ./app

ENV PYTHONPATH=/app/common

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
