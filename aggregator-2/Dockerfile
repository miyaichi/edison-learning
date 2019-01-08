FROM centos

RUN cp -p  /usr/share/zoneinfo/Japan /etc/localtime

RUN cd /root/
RUN curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
RUN python get-pip.py
RUN pip install awscli
RUN mkdir /root/.aws
ADD aws/config /root/.aws/
ADD aws/credentials /root/.aws/

RUN yum -y update
RUN yum -y install epel-release
RUN yum -y install postgresql postgresql-devel nodejs npm
RUN npm install aws-sdk date-utils mktemp pg
ADD aggregator.js /root/
ADD config.json /root/
