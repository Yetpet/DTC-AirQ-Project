FROM python:3.11-slim

# Install dependencies in one layer to reduce runtime dependencies in kestra
RUN pip install --no-cache-dir \
    requests==2.33.1 \
    google-cloud-storage==3.10.1 \
    google-auth==2.49.1 \
    && rm -rf /root/.cache/pip

WORKDIR /app

CMD ["python"]
