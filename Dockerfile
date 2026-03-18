FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY nrp_accounting_pipeline ./nrp_accounting_pipeline
COPY etl.py backfill.py institution_import.py ./

ENTRYPOINT ["python", "etl.py"]
