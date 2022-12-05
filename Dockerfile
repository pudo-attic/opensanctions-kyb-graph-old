FROM ubuntu:22.04

LABEL org.opencontainers.image.title "OpenSanctions Graph ETL"
LABEL org.opencontainers.image.source https://github.com/opensanctions/graph

RUN apt-get -qq -y update \
    && apt-get -qq -y upgrade \
    && apt-get -qq -y install locales ca-certificates curl python3-pip \
    python3-icu python3-cryptography vim unzip lsb-release python3-lxml \
    libicu-dev pkg-config git wget curl \
    && apt-get -qq -y autoremove \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG="en_US.UTF-8"
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir -p /graph
COPY . /graph
RUN pip install -e /graph
WORKDIR /graph
CMD ["bash"]
