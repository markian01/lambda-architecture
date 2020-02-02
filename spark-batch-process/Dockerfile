FROM bde2020/spark-python-template:2.4.4-hadoop2.7

RUN apk update && apk add docker
RUN echo '* * * * * sh /template.sh  > /dev/stdout' >> /etc/crontabs/root

CMD sh /wait-for-init.sh && crond -l 2 -f
