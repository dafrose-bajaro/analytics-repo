FROM python:3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_VERSION=0.6.13
ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

WORKDIR /tmp

SHELL [ "/bin/bash", "-euxo", "pipefail", "-c" ]
RUN apt-get install -y --no-install-recommends curl ca-certificates

ADD https://astral.sh/uv/${UV_VERSION}/install.sh install-uv.sh

SHELL [ "/bin/sh", "-eu", "-c" ]
RUN chmod +x /tmp/install-uv.sh && /tmp/install-uv.sh

WORKDIR /app

ENTRYPOINT [ "/bin/bash", "-euxo", "pipefail", "-c" ]