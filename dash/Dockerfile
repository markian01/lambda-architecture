FROM python:3.6

RUN apt-get -yqq update && apt-get -yqq install docker.io

WORKDIR /app
ADD ./ ./
RUN pip install -r requirements.txt

CMD sh /wait-for-init.sh && python3 app.py
