# Use Python 3.10 instead of the latest
FROM python:3.10

RUN apt-get update && apt-get install -y build-essential gcc python3-dev libffi-dev

WORKDIR /app
COPY . /app

# Install Python dependencies
RUN python3 -m pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt
