FROM xserrat/facebook-demucs:latest

RUN pip3 install --no-cache-dir --upgrade minio redis requests

WORKDIR /app
COPY worker-server.py /app/worker-server.py

ENTRYPOINT ["python3", "/app/worker-server.py"]
