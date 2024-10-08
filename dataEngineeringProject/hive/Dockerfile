FROM bde2020/hadoop-base:2.0.0-hadoop2.7.4-java8

# Allow build-time configuration of HIVE_VERSION
ARG HIVE_VERSION
# Set HIVE_VERSION from arg if provided at build, env if provided at run, or default to 2.3.2
ENV HIVE_VERSION=${HIVE_VERSION:-2.3.2}

# Set environment variables for Hive and Hadoop
ENV HIVE_HOME /opt/hive
ENV PATH $HIVE_HOME/bin:$PATH
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION

WORKDIR /opt

# Install Hive and PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y wget procps && \
    wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz && \
    tar -xzvf apache-hive-$HIVE_VERSION-bin.tar.gz && \
    mv apache-hive-$HIVE_VERSION-bin hive && \
    wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jar -O $HIVE_HOME/lib/postgresql-jdbc.jar && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz && \
    apt-get --purge remove -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy custom configuration files
ADD conf/hive-site.xml $HIVE_HOME/conf/
ADD conf/beeline-log4j2.properties $HIVE_HOME/conf/
ADD conf/hive-env.sh $HIVE_HOME/conf/
ADD conf/hive-exec-log4j2.properties $HIVE_HOME/conf/
ADD conf/hive-log4j2.properties $HIVE_HOME/conf/
ADD conf/ivysettings.xml $HIVE_HOME/conf/
ADD conf/llap-daemon-log4j2.properties $HIVE_HOME/conf/

# Copy startup and entrypoint scripts and make them executable
COPY startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/startup.sh

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Expose Hive services
EXPOSE 10000  
EXPOSE 10002  

# Set entrypoint and default command
ENTRYPOINT ["entrypoint.sh"]
CMD ["startup.sh"]
