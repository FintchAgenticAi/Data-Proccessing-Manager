FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/source code"

WORKDIR /app

COPY ["source code/requirements.txt", "requirements.txt"]
RUN pip install --no-cache-dir -r requirements.txt

COPY ["source code", "./source code"]

EXPOSE 8002

CMD ["uvicorn", "app.main:app", "--app-dir", "source code", "--host", "0.0.0.0", "--port", "8002", "--reload"]
