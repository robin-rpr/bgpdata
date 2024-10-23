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
    librdkafka-dev \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .
