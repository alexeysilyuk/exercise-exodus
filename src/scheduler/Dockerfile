# /src/scheduler/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Copy the common requirements.txt
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Run the Scheduler script
CMD ["python", "scheduler.py"]