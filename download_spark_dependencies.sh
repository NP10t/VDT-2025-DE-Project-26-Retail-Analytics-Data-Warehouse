#!/bin/bash

mkdir -p jars

urls=(
  # "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar"
  # "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar"
  # "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar"
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.533/aws-java-sdk-bundle-1.12.533.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  # "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
  "https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.4_2.12/0.8.0/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar"
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-client/0.7.0/clickhouse-client-0.7.0.jar"
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-http-client/0.7.0/clickhouse-http-client-0.7.0.jar"
  "https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.2.1/httpclient5-5.2.1.jar"
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar"
  "https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.1/httpcore5-5.2.1.jar"
  "https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar"
  "https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.13/httpcore-4.4.13.jar"
)

for url in "${urls[@]}"; do
  filename=$(basename "$url")
  if [ -f "jars/$filename" ]; then
    echo "Skipping $filename (already exists)"
    continue
  fi
  echo "Downloading $url"
  wget -q --show-progress -P jars "$url"
done

echo "✅ All JARs downloaded to ./jars"