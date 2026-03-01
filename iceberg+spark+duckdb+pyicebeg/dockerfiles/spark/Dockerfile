FROM apache/spark:4.1.1-scala2.13-java21-python3-r-ubuntu

USER root
RUN apt-get update && apt-get -y upgrade

ENV PATH=${SPARK_HOME}/bin:$PATH
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:${PYTHONPATH}"

RUN mkdir /nonexistent
RUN chown spark /nonexistent
