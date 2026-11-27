FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1
# On Apple Silicon the JDK lives in the arm64 folder instead.
RUN [ -d "$JAVA_HOME" ] || ln -s /usr/lib/jvm/java-17-openjdk-arm64 "$JAVA_HOME"

RUN pip install --no-cache-dir pyspark==3.5.3 pandas==2.2.3
# Parts 2-4 (producer, stream sink, loaders, report) run from this image too.
RUN pip install --no-cache-dir kafka-python==3.0.13 "psycopg[binary]==3.3.9"

WORKDIR /app
COPY jobs/ /app/jobs/
COPY quality/ /app/quality/
COPY serve/ /app/serve/
COPY stream/ /app/stream/
COPY scripts/ /app/scripts/
# quality/batch_checks.py imports both Part 1's common.py and quality/rules.py
ENV PYTHONPATH=/app:/app/jobs
COPY run_part1.sh /app/run_part1.sh
WORKDIR /app/jobs
CMD ["bash", "/app/run_part1.sh"]
