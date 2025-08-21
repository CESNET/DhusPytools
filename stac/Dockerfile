FROM python:3.13-slim

WORKDIR /app

COPY stac_sync.py requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "stac_sync.py"]