# Use an official Python runtime as the base image
FROM python:3.11-slim

# Set environment variables
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app


# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential \
    curl \
    apt-utils \
    gnupg2 &&\
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip



# Copy only requirements first to leverage Docker caching
COPY requirements.txt ./

# Install dependencies with no cache to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . ./

# Install dependencies with no cache to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Flask runs on
EXPOSE 8080

# Command to run the application
# CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8080", "--timeout", "0", "main:app"]
CMD python main.py