FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

RUN hdfs dfs -mkdir -p /kafka/topics
RUN hdfs dfs -mkdir -p /kafka/logs
RUN hdfs dfs -mkdir -p /spark-logs

EXPOSE 9870
EXPOSE 8020
EXPOSE 9000

CMD ["/bin/bash", "/run.sh"]


