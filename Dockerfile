FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app/src

CMD ["python", "-m", "kalshi_weather.cli", "scan", "--notify"]
