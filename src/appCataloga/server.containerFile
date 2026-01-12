FROM python:3.13-slim

# Environment vars for Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install deps for pandas and SFTP client
RUN apt-get update && apt-get install -y --no-install-recommends \
      openssh-client \
      build-essential \
      libatlas-base-dev \
      libopenblas-dev \
      liblapack-dev \
      tzdata \
 && rm -rf /var/lib/apt/lists/*

# Install Python libs
RUN pip install --no-cache-dir --upgrade pip \
 && pip install pandas paramiko pysftp

# Create app directory
WORKDIR /app

# Copy your Python scripts (e.g. fetch_sftp.py)
COPY . /app

# Default command (can be overridden)
CMD ["python", "fetch_sftp.py"]