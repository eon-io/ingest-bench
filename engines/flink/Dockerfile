# Compile architecture-independent bytecode on the build host to avoid emulation.
FROM --platform=$BUILDPLATFORM maven:3.9.9-eclipse-temurin-17 AS runner-build
WORKDIR /build
COPY engines/flink/sql-runner/pom.xml .
COPY engines/flink/sql-runner/src ./src
RUN mvn -B -q package

# The maintenance sidecar's job: Iceberg's TableMaintenance driven from a rendered document.
FROM --platform=$BUILDPLATFORM maven:3.9.9-eclipse-temurin-17 AS maintenance-build
WORKDIR /build
COPY engines/flink/maintenance-runner/pom.xml .
COPY engines/flink/maintenance-runner/src ./src
RUN mvn -B -q package

FROM flink:2.2.1-scala_2.12-java17

ARG ICEBERG_VERSION=1.12.0
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG KAFKA_CLIENTS_VERSION=4.2.0
ARG MSK_IAM_AUTH_VERSION=2.3.8
# Use the compression codecs and versions declared by kafka-clients.
ARG ZSTD_JNI_VERSION=1.5.6-10
ARG LZ4_JAVA_VERSION=1.10.1
ARG SNAPPY_JAVA_VERSION=1.1.10.7
ARG FLINK_VERSION=2.2.1
ARG HADOOP_VERSION=3.3.6

# Enable the S3 plugin for checkpoints. Iceberg table data uses its own FileIO.
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-${FLINK_VERSION}.jar

USER root
COPY LICENSE NOTICE /opt/bench/
COPY LICENSES /opt/bench/LICENSES/
WORKDIR /opt/flink/lib
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.2/${ICEBERG_VERSION}/iceberg-flink-runtime-2.2-${ICEBERG_VERSION}.jar .
# Supply the AWS and GCP SDKs used by Iceberg's runtime.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# MSK's IAMClientCallbackHandler requires unshaded Kafka interfaces. Use the
# plain connector and client; the shaded SQL jar relocates those interfaces.
# The plain connector still registers KafkaDynamicTableFactory.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/github/luben/zstd-jni/${ZSTD_JNI_VERSION}/zstd-jni-${ZSTD_JNI_VERSION}.jar .
ADD https://repo1.maven.org/maven2/at/yawk/lz4/lz4-java/${LZ4_JAVA_VERSION}/lz4-java-${LZ4_JAVA_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/${SNAPPY_JAVA_VERSION}/snappy-java-${SNAPPY_JAVA_VERSION}.jar .
# The all classifier includes AWS SDK v2 for pod identity credentials, unshaded and
# older (2.51) than iceberg-aws-bundle's. Flink orders lib/ by file name, so the jar is
# saved under a name that sorts after iceberg-aws-bundle and Iceberg's newer SDK wins;
# otherwise Iceberg's S3 client fails with NoSuchMethodError in AwsClientEndpointProvider.
ADD https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar
# This jar registers both avro and avro-confluent. Adding flink-sql-avro too
# would duplicate shaded Avro classes.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar .
# Iceberg needs Hadoop Configuration even with object-store FileIO. Flink's
# HadoopModule also initializes UserGroupInformation and its dependencies.
# The shaded client pair supplies that dependency tree without separate jars.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar .
RUN chown -R flink:flink /opt/flink/lib && chmod 644 /opt/flink/lib/*.jar

COPY --from=runner-build --chown=flink:flink /build/target/sql-runner.jar /opt/bench/sql-runner.jar
COPY --from=maintenance-build --chown=flink:flink /build/target/maintenance-runner.jar /opt/bench/maintenance-runner.jar
USER flink
WORKDIR /opt/flink
