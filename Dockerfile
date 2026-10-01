# syntax=docker/dockerfile:1
FROM python:3.14-slim@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d

# TTFH_HOST: the app defaults to 127.0.0.1; inside the container it must listen on
# all interfaces. compose publishes no host port; it is reachable only on "ttfeed".
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src \
    TTFH_HOST=0.0.0.0

WORKDIR /app
COPY requirements.lock .
RUN pip install --root-user-action=ignore --require-hashes --no-deps -r requirements.lock \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin ttfh
COPY src/ ./src/

USER 10001
EXPOSE 8700
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import sys, urllib.request as u; sys.exit(0 if u.urlopen('http://127.0.0.1:8700/health', timeout=2).status == 200 else 1)"]
CMD ["python", "-m", "ttfeedhub"]
