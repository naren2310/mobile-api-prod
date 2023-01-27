FROM python:3.8-alpine

# Set the working directory
WORKDIR /Mobile_Api_Prod/mobile_api_validateOTP

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port for the application
EXPOSE 5000

# Start the application using Gunicorn
CMD exec gunicorn --workers 1 --threads 1 --timeout 0 --bind 0.0.0.0:5000 main:app