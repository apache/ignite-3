FROM ubuntu:22.04
# Install dependencies
RUN apt update 
RUN apt install -y git gcc g++ clang make cmake unixodbc unixodbc-dev odbcinst libssl-dev openjdk-11-jdk openjdk-17-jdk rpm
RUN apt clean
