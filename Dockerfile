FROM python:3.10.9-slim

# Install Postgres as pre requisite
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y lsb-release && \
    apt-get install -y wget && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get install -y postgresql && \
    apt-get install -y libpq-dev && \
    apt-get clean

RUN mkdir patsy
WORKDIR /patsy

COPY requirements.txt .
COPY patsy ./patsy
COPY README.md .
COPY setup.cfg .
COPY setup.py .
COPY LICENSE .

RUN pip install -e .[dev,test]

ENTRYPOINT [ "patsy" ]
