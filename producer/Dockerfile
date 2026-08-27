FROM python:3.10-slim

WORKDIR /producer

COPY app.py .
COPY solarbi.py .
COPY utils.py .
COPY active_sites.py .

COPY requirements.txt .
RUN pip install -r requirements.txt

# CMD ["python", "app.py", "--start-date", "2025-07-01", "--end-date", "2025-07-3"]
CMD ["python", "app.py"]
