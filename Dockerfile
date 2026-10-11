# --- stage 1: build the visualizer UI (Node is only needed here, never at runtime) ---------------------
FROM node:24-slim AS ui
WORKDIR /ui
COPY viz-ui/package.json viz-ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY viz-ui/ ./
RUN VIZ_OUT_DIR=/ui-dist npm run build

# --- stage 2: the Python image used by every service ---------------------------------------------------
FROM python:3.14.5-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=ui /ui-dist /srv/celery_viz/ui_dist

# Celery refuses to run as root without C_FORCE_ROOT, and rightly so.
RUN useradd --create-home --uid 1000 pipeline
USER pipeline
