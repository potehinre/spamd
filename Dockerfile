FROM debian:buster-slim
RUN apt-get update 
RUN apt-get install -y \
  git \
  python3 \
  python3-pip
ADD *.py /app/
ADD requirements.txt /app/
ADD emails.csv /app/
WORKDIR /app
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python3", "main.py"]
