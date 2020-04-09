# Code Written by Evan West
# With contributions from: Biawan Huang, Bryan Ji, and Eugene Chou
# Publicly posted to github
# https://github.com/etwest

# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file, do this in this order to allow for faster image building
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Make port 80 available to the world outside this container
EXPOSE 8080
# Define environment variable
ENV NAME World \
    MAINIP \
    IP_PORT

# Run app.py when the container launches
CMD ["python3", "app.py"]
