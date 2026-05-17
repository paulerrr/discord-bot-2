FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logger.py .

CMD ["sh", "-c", "mkdir -p data && exec python logger.py"]
