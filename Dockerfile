FROM python:3.14.5-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Celery refuses to run as root without C_FORCE_ROOT, and rightly so.
RUN useradd --create-home --uid 1000 pipeline
USER pipeline
