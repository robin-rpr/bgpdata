FROM python:3.6

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Define environment variables with default values
ENV POSTGRESQL_DATABASE=postgresql://postgres:5432/default
ENV TIMESCALE_DATABASE=postgresql://timescale:5432/default
ENV POSTMARK_API_KEY=your-postmark-api-key
ENV FLASK_SECRET_KEY=your-flask-secret-key
ENV FLASK_HOST=https://fombook.com
ENV FLASK_ENV=production
ENV FLASK_DEBUG=0
ENV FLASK_APP=app.py
ENV FLASK_RUN_PORT=80

# Command to run the application with Gunicorn
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:80", "app:app"]
