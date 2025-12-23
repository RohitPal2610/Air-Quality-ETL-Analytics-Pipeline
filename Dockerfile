FROM apache/airflow:2.8.3

USER root

# 1. Add the Debian Sid repository to find the older Java 8 package
# 2. Install OpenJDK 8 (Java 1.8.0) and cleanup to keep the image small
RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common dirmngr && \
    echo "deb http://http.debian.net/debian sid main" | tee /etc/apt/sources.list.d/sid.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Set the JAVA_HOME variable to the Linux path for Java 1.8
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# 4. Add Java to the system PATH
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# 5. Install PySpark for processing
# 6. Install Pandas and openpyxl for reading the .xlsx dataset
RUN pip install --no-cache-dir pyspark pandas openpyxl