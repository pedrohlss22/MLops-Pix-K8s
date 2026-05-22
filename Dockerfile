FROM python:3.10-slim

WORKDIR /app

COPY src/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -u 1001 -m apiuser
COPY --chown=1001:1001 src/api/main.py src/api/

USER apiuser
EXPOSE 8000


CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]