FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY data ./data

ENV PYTHONPATH=/app

CMD ["uvicorn", "src.interfaces.app:app", "--host", "0.0.0.0", "--port", "8000"]
