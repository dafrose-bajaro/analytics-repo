# Overview of Dockerfile

How does the Dockerfile work? Let's deconstruct its components. 

1) **Base Image**<br>
`FROM python:3.12-bookworm`<br>
This builds Python 3.12 on top of Debian.<br><br>

2) **Environment**<br>
`ENV DEBIAN_FRONTEND=noninteractive`<br>
`ENV PYTHONDONTWRITEBYTECODE=1`<br>
`ENV PYTHONUNBUFFERED=1`<br>
`ENV UV_VERSION=0.6.13`<br>
`ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"`<br>
This configures environment variables for automated installations. <br><br>

3) **Working Directory**<br>
`WORKDIR /tmp`<br>
`WORKDIR /app`<br>
This defines directroeis for temporary operations and final application setup. <br><br>

4) **Shell Configuration**<br>
`SHELL [ "/bin/bash", "-euxo", "pipefail", "-c" ]`<br>
`SHELL [ "/bin/sh", "-eu", "-c" ]`<br>
This sets error-handling rules to ensure commands fail immeidately if there are any issues. <br><br>

5) **Dependencies**<br>
`RUN apt-get install -y --no-install-recommends curl ca-certificates`<br>
`RUN chmod +x /tmp/install-uv.sh && /tmp/install-uv.sh`<br>
This installs system tools like `curl` and SSL certificates. <br><br>

6) **External Scripts**<br>
`ADD https://astral.sh/uv/${UV_VERSION}/install.sh install-uv.sh`<br>
This installs UV, a Python package manager that is more efficient than `pip`.<br><br>

7) **Entry Point**<br>
`ENTRYPOINT [ "/bin/bash", "-euxo", "pipefail", "-c" ]`<br>
This run bash upon startup of the container. 