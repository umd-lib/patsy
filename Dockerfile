FROM python:3.10.9-slim

# Install Postgres as pre requisite
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y git && \
    apt-get install -y lsb-release && \
    apt-get install -y wget && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get -y install postgresql && \
    apt-get -y install libpq-dev && \
    apt-get clean

RUN mkdir patsy
WORKDIR /patsy

ADD requirements.txt .
ADD patsy ./patsy
ADD README.md .
ADD setup.cfg .
ADD setup.py .
ADD LICENSE .

RUN pip install -e .[dev,test]
