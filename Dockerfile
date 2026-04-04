# Start from official Airflow image
FROM apache/airflow:2.7.0

# Switch to root to install OS-level packages
USER root

# Install Postgres client libraries (needed for psycopg2)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements.txt into the image
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt