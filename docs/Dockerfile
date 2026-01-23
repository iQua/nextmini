FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /opt

RUN apk add bash

COPY . .

RUN uv venv /opt/.venv --python 3.13 --clear

RUN source .venv/bin/activate && uv pip install -r requirements.txt

EXPOSE 8000

CMD [ "/bin/bash", "-c", "source /opt/.venv/bin/activate && mkdocs serve" ]