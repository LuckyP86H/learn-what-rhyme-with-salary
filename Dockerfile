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
# eMudhra/emSign CA incident (Oct 2026): the pip bundled with Python 3.14.5
# predates pip 26.2.2 and keeps trusting the compromised emSign CAs (pip
# always loads its vendored certifi in addition to the system store).
# Bootstrap a fixed pip from the PSF advisory's hash-pinned direct URL
# before installing anything.
RUN pip install --no-cache-dir "pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=ui /ui-dist /srv/celery_viz/ui_dist

# Celery refuses to run as root without C_FORCE_ROOT, and rightly so.
RUN useradd --create-home --uid 1000 pipeline
USER pipeline
