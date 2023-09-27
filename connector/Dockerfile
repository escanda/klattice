FROM rust:1.67

WORKDIR /usr/src/myapp

COPY ./proto ./proto

COPY ./core/src/main/proto/* ./proto/

COPY ./connector/ .

COPY ./connector/build.docker.rs build.rs

## Add the wait script to the image
COPY --from=ghcr.io/ufoscout/docker-compose-wait:latest /wait /wait

RUN apt-get update

RUN apt-get install -y protobuf-compiler

RUN cargo install --path .

CMD /wait && connector
