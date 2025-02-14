FROM nvidia/cuda:11.8.0-base-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    software-properties-common \
    && add-apt-repository ppa:ubuntuhandbook1/ffmpeg6 \
    && apt-get update \
    && apt-get install -y ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
COPY app/*.py /app/
RUN pip3 install -r /app/requirements.txt

WORKDIR /app
RUN mkdir -p ./hls ./cache ./content && chmod 777 ./hls ./cache ./content
    
CMD ["python3", "-u", "main.py"]