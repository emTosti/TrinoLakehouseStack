# Schema-proposer / data inspector.
#
# Read-only service: watches the `landing` bucket for new objects, samples a
# bounded slice of each one, and writes a *proposed* schema (with examples) to
# the `inspection-reports` bucket. It never registers anything and has no route
# to the catalog or query engine.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin inspector

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

USER inspector
EXPOSE 8000
CMD ["python", "app.py"]
