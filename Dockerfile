FROM python:3.11.3

# Define environment variables with default values
ENV POSTGRESQL_DATABASE=postgresql+asyncpg://postgres:5432/default
ENV TIMESCALE_DATABASE=postgresql+asyncpg://timescale:5432/default
ENV POSTMARK_API_KEY=your-postmark-api-key
ENV FLASK_SECRET_KEY=your-flask-secret-key
ENV FLASK_HOST=https://bgpdata.io
ENV FLASK_ENV=production
ENV FLASK_DEBUG=0
ENV FLASK_APP=app.py
ENV FLASK_RUN_PORT=80

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    zlib1g-dev \
    libbz2-dev \
    libcurl4-openssl-dev \
    librdkafka-dev \
    autoconf \
    automake \
    libtool \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Wandio from source
RUN mkdir -p /tmp/wandio && \
    cd /tmp/wandio && \
    curl -LO https://github.com/LibtraceTeam/wandio/archive/refs/tags/4.2.4-1.tar.gz && \
    tar zxf 4.2.4-1.tar.gz && \
    cd wandio-4.2.4-1/ && \
    autoreconf -fiv && \
    ./configure && \
    make && \
    make install && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/wandio

# Install BGPStream from source
RUN mkdir -p /tmp/bgpstream && \
    cd /tmp/bgpstream && \
    curl -LO https://github.com/CAIDA/libbgpstream/releases/download/v2.2.0/libbgpstream-2.2.0.tar.gz && \
    tar zxf libbgpstream-2.2.0.tar.gz && \
    cd libbgpstream-2.2.0/ && \
    autoreconf -fiv && \
    ./configure && \
    make && \
    make check && \
    make install && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/bgpstream

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Run the application with Gunicorn
CMD ["sh", "-c", "alembic upgrade head && gunicorn --bind 0.0.0.0:80 --workers 4 --worker-class uvicorn.workers.UvicornWorker 'app:asgi_app'"]