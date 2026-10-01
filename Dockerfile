FROM python:3.10.9-slim

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
