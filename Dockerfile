FROM python:3.11-slim

# System packages 
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    iputils-ping \
    xmlstarlet \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Clone repo with scripts
RUN git clone https://github.com/CESNET/DhusPytools.git .

# Python dependecies
RUN pip install --no-cache-dir -r requirements.txt

# Copying entrypoint script that generates .netrc from .env variables
COPY start_script.sh /app/start_script.sh
RUN chmod +x /app/start_script.sh

# Setting up the entrypoint (this creates a .netrc) and then running our Python script.
ENTRYPOINT ["/app/start_script.sh"]

# When the container starts, the Python script check_new_register_stac.py is run
CMD ["python3", "/app/check_new_register_stac.py"]
