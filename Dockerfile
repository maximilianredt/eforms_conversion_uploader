FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py config.py bq_client.py google_ads_client.py microsoft_ads_client.py queries.py ./

CMD ["python", "main.py"]
