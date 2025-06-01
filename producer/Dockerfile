FROM python:3.10-slim

WORKDIR /producer

COPY app.py .
COPY solarbi.py .
COPY utils.py .

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "app.py"]

