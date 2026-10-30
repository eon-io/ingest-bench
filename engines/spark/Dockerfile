# Stock Spark and its connectors support amd64 and arm64 without a platform pin.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.1.3
ARG SCALA_BINARY=2.13
ARG ICEBERG_VERSION=1.12.0
# Match the Kafka client version declared by this Spark connector.
ARG KAFKA_CLIENTS_VERSION=3.9.1
# Spark ships commons-pool 1.x; the Kafka consumer pool requires 2.x.
ARG COMMONS_POOL2_VERSION=2.12.1
ARG MSK_IAM_AUTH_VERSION=2.3.8
# Match hadoop-aws to the hadoop-client jars bundled with Spark.
ARG HADOOP_VERSION=3.4.2
# Hadoop 3.4's S3A runs on AWS SDK v2 and needs more of it than Iceberg's AWS
# bundle carries (the transfer manager and the Apache HTTP client). Use the
# SDK version Iceberg is built against, so one SDK version is on the classpath.
ARG AWS_SDK_V2_VERSION=2.54.17
# Upsert verification registers frozen metadata in a session-local JDBC catalog. Derby, which
# Spark bundles, rejects the LIMIT queries Iceberg's JDBC catalog issues; SQLite does not.
ARG SQLITE_JDBC_VERSION=3.47.1.0

USER root
COPY LICENSE NOTICE /opt/bench/
COPY LICENSES /opt/bench/LICENSES/
WORKDIR /opt/spark/jars
# Remote ADD files arrive with mode 600. Setting the mode on ADD, instead of a
# later chmod layer, keeps the image from holding every jar twice.
# MSK's IAMClientCallbackHandler requires unshaded Kafka interfaces.
# Spark already bundles the client's three compression codecs.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar .
# from_avro is in spark-avro, not the bundled Avro core jar.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_BINARY}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY}-${SPARK_VERSION}.jar .
# Iceberg's cloud integrations also need these AWS and GCP SDK bundles.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_${SCALA_BINARY}/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_${SCALA_BINARY}-${ICEBERG_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# The all classifier includes AWS SDK v2 for pod identity credentials.
ADD --chmod=644 https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar .
# Checkpoints use Hadoop S3A; table files use Iceberg FileIO. Both clients
# are required.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar .
ADD --chmod=644 https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_JDBC_VERSION}/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar .

# Preserve package paths for imports. Omit the renderer and its harness dependencies.
ENV PYTHONPATH=/opt/bench
COPY engines/__init__.py /opt/bench/engines/
COPY engines/spark/__init__.py engines/spark/env.py engines/spark/stream_to_iceberg.py engines/spark/upsert_scan.py /opt/bench/engines/spark/
# The rewrite sidecar runs the same image; only its entry script differs.
COPY compactors/__init__.py /opt/bench/compactors/
COPY compactors/spark_rewrite/__init__.py compactors/spark_rewrite/rewrite_loop.py /opt/bench/compactors/spark_rewrite/
USER spark
WORKDIR /opt/spark
