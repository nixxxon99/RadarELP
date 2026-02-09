FROM python:3.11-slim

WORKDIR /app/elp_radar_bot

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY elp_radar_bot/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY elp_radar_bot/app ./app

CMD ["python", "-m", "app.main"]
