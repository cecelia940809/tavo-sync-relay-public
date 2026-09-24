FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
ENV PORT=8787
EXPOSE 8787
CMD ["python", "server.py"]
