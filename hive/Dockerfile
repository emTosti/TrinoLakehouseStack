# Apache Hive Standalone Metastore for Trino's `s3` (Hive) catalog.
#
# Two adjustments over the upstream image:
#  1. Bundle the PostgreSQL JDBC driver so the metastore can keep its schema in
#     the shared `postgres` service (database `metastore`).
#  2. Put hadoop-aws + the AWS SDK on the metastore classpath so it can create
#     schema/table directories on MinIO (S3A). Both jars already ship in the
#     image under hadoop's tools/lib, which is not on the default classpath.
FROM apache/hive:4.0.0

USER root
ADD --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /opt/hive/lib/postgresql-42.7.4.jar
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar \
       /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar \
       /opt/hive/lib/
USER hive
