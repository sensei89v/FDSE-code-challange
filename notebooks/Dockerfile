FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY live_metrics.py .

EXPOSE 8888

CMD ["marimo", "run", "live_metrics.py"]
