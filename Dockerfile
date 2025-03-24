FROM public.ecr.aws/docker/library/python:3.11.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH=/app \
    SSL_CERT_DIR=/etc/pki/ca-trust/source/anchors

# Set working directory
WORKDIR /app

# Install system dependencies and AWS CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    ca-certificates \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Install AWS CLI v2
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip

# Create necessary directories with correct permissions
RUN mkdir -p /etc/pki/ca-trust/source/anchors/ \
    && chmod 755 /etc/pki/ca-trust/source/anchors/

# Download and install AWS RDS CA certificate
RUN curl -o /etc/pki/ca-trust/source/anchors/rds_ca.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    && chmod 644 /etc/pki/ca-trust/source/anchors/rds_ca.pem \
    && update-ca-certificates

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && mkdir -p /home/appuser/.aws \
    && chown -R appuser:appuser /app /home/appuser \
    && chmod -R 755 /etc/pki/ca-trust/source/anchors/

# Copy only the necessary source code
COPY --chown=appuser:appuser src/ src/

# Expose the port the app runs on
EXPOSE 8000

# Switch to non-root user
USER appuser

# Command to run the application
CMD ["python", "-m", "src.main"] 