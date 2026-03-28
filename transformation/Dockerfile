FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transformation.py .

CMD ["sh", "-c", "python transformation.py --target-url \"$TARGET_URL\" --host 0.0.0.0 --port 8081 --window ${WINDOW:-5}"]
