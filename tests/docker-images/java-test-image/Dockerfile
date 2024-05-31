#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG PULSAR_IMAGE
FROM $PULSAR_IMAGE

# Base pulsar image is designed not be modified, though we need to add more scripts
USER root

COPY target/scripts /pulsar/bin
RUN chmod a+rx /pulsar/bin/*

ARG DEBIAN_FRONTEND=noninteractive
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu/

RUN sed -i "s|http://archive\.ubuntu\.com/ubuntu/|${UBUNTU_MIRROR:-mirror://mirrors.ubuntu.com/mirrors.txt}|g" /etc/apt/sources.list \
     && apt-get update \
     && apt-get -y dist-upgrade \
     && apt-get install -y supervisor

RUN mkdir -p /var/log/pulsar \
    && mkdir -p /var/run/supervisor/ \
    && mkdir -p /pulsar/ssl

COPY target/conf /etc/supervisord/conf.d/
RUN mv /etc/supervisord/conf.d/supervisord.conf /etc/supervisord.conf

COPY target/ssl /pulsar/ssl/

COPY target/java-test-functions.jar /pulsar/examples/

# cleanup
RUN apt-get -y --purge autoremove \
    && apt-get autoclean \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
