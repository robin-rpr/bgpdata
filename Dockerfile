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
    openmpi-bin \
    libopenmpi-dev \
    openssh-server \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure SSH server
RUN mkdir /var/run/sshd && \
    echo "StrictModes no" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "PermitEmptyPasswords yes" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "ChallengeResponseAuthentication no" >> /etc/ssh/sshd_config

# Configure SSH client
RUN ssh-keygen -A && \
    echo "Host *" >> /etc/ssh/ssh_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "PreferredAuthentications password" >> /etc/ssh/ssh_config && \
    echo "PubkeyAuthentication no" >> /etc/ssh/ssh_config

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Remove root password
RUN passwd -d root
