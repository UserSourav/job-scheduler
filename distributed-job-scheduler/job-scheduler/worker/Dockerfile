FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

ENTRYPOINT ["python", "worker.py"]
CMD ["--api-base", "http://backend:8000", "--queues", "default", "--concurrency", "5"]
