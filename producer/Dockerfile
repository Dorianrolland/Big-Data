FROM python:3.12-slim

WORKDIR /app

# Couche de dépendances (cachée si requirements.txt ne change pas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# -u : unbuffered stdout pour que les logs apparaissent en temps réel
CMD ["python", "-u", "main.py"]
