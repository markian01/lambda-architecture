FROM bde2020/spark-python-template:2.4.4-hadoop2.7

RUN apk update && apk add docker

CMD sh /wait-for-init.sh && sh /template.sh
