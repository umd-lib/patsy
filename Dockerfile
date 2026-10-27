FROM python:3.14.9-slim

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

WORKDIR /opt/patsy

COPY pyproject.toml .
COPY patsy ./patsy

# Temporarily mount the .git folder to infer the version during installation
RUN --mount=source=.git,target=.git,type=bind \
    pip install -e .

ENTRYPOINT [ "patsy" ]
