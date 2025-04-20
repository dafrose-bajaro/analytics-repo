# Overview of Dockerfile

How does the Dockerfile work? Let's deconstruct its components. 

1) **Base Image**
`FROM python:3.12-bookworm`
This builds Python 3.12 on top of Debian.<br><br>

2) **Environment**
`ENV DEBIAN_FRONTEND=noninteractive`
`ENV PYTHONDONTWRITEBYTECODE=1`
`ENV PYTHONUNBUFFERED=1`
`ENV UV_VERSION=0.6.13`
`ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"`
This configures environment variables for automated installations. <br><br>

3) **Working Directory**
`WORKDIR /tmp`
`WORKDIR /app`
This defines directroeis for temporary operations and final application setup. <br><br>

4) **Shell Configuration**
`SHELL [ "/bin/bash", "-euxo", "pipefail", "-c" ]`
`SHELL [ "/bin/sh", "-eu", "-c" ]`
This sets error-handling rules to ensure commands fail immeidately if there are any issues. <br><br>

5) **Dependencies**
`RUN apt-get install -y --no-install-recommends curl ca-certificates`
`RUN chmod +x /tmp/install-uv.sh && /tmp/install-uv.sh`
This installs system tools like `curl` and SSL certificates. <br><br>

6) **External Scripts**
`ADD https://astral.sh/uv/${UV_VERSION}/install.sh install-uv.sh`
This installs UV, a Python package manager that is more efficient than `pip`.<br><br>

7) **Entry Point**
`ENTRYPOINT [ "/bin/bash", "-euxo", "pipefail", "-c" ]`
This run bash upon startup of the container. 