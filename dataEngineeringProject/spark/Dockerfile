# Use a lightweight OpenJDK base image
FROM openjdk:11-jre-slim

# Define Spark and Hadoop versions
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install necessary dependencies
RUN apt update && \
    apt-get install -y --no-install-recommends \
        curl procps make build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev wget llvm \
        libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev \
        python3-pip \
        liblzma-dev python3-openssl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pyenv and Python 3.12
RUN curl https://pyenv.run | bash && \
    export PYENV_ROOT="$HOME/.pyenv" && \
    export PATH="$PYENV_ROOT/bin:$PATH" && \
    export PATH="$PYENV_ROOT/versions/3.12/bin:$PATH" && \
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc && \
    echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc && \
    echo 'eval "$(pyenv init --path)"' >> ~/.bashrc && \
    echo 'eval "$(pyenv init -)"' >> ~/.bashrc && \
    eval "$(pyenv init --path)" && \
    eval "$(pyenv init -)" && \
    pyenv install 3.12 && \
    pyenv global 3.12


# Install Python dependencies
RUN pip install requests sentry-sdk==0.10.2

# Download and extract Apache Spark
RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Copy Spark configuration files
COPY config /etc/spark

# Copy the entrypoint script and set permissions
COPY scripts/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Create the directory and placeholder file for the Sentry DSN
RUN mkdir -p /etc/spark && \
    echo "http://bdee0659227b4a5fa6ef33a7c18b5952@sentry-sentry-1:9000/1" > /etc/spark/sentry_dsn.txt && \
    chmod 644 /etc/spark/sentry_dsn.txt

# Set the entrypoint script
ENTRYPOINT ["/opt/entrypoint.sh"]
