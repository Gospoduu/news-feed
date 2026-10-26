FROM ubuntu:latest
LABEL authors="tema"

ENTRYPOINT ["top", "-b"]