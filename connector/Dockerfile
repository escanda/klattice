FROM rust:1.67

WORKDIR /usr/src/myapp
COPY ./proto ./proto
COPY ./core/src/main/proto/* ./proto/
COPY ./connector/ .
COPY ./connector/build.docker.rs build.rs

RUN apt-get update
RUN apt-get install -y protobuf-compiler

RUN cargo install --path .

CMD ["connector"]
