FROM python:3.11.4

RUN apt-get update && apt-get install -y nodejs npm g++

# Install dbt
RUN pip3 --disable-pip-version-check --no-cache-dir install duckdb==0.8.1 dbt-duckdb==1.1.4 \
    && rm -rf /tmp/pip-tmp

# Install duckdb cli
RUN wget https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

WORKDIR /workspaces/datadex
COPY duckdb_serve.py .
ENV DBT_PROFILES_DIR=/workspaces/datadex
ENV DUCKDB_SERVER_PORT=8090
CMD ["python", "duckdb_serve.py"]
