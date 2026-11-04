# UrbanStream Spark 이미지 (Step 4). 공식 apache/spark 이미지 위에 "Kafka 커넥터 jar + 우리 코드"를 얹는다.
# 결정 근거: docs/adr/0005-bronze-spark.md
#
# 빌드 컨텍스트는 프로젝트 루트다 (docker-compose.yml 의 spark-bronze.build). 루트의 .dockerignore 가 .env 와 data/ 를 제외한다.
# 되돌리기/버전 올리기: SPARK_IMAGE, SPARK_VERSION 두 값만 바꾼다 (.env 또는 --build-arg).
#   Scala 버전(SCALA_BINARY)은 이미지 태그의 scala 부분과 같아야 한다. Spark 3.5 계열은 2.12, 4.x 는 2.13.
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG SPARK_VERSION=4.1.3
ARG SCALA_BINARY=2.13

USER root

# python3-yaml: config/*.yaml 읽기, python3-pytest: tests/spark 를 이 이미지(운영과 같은 Spark/Java)에서 돌리기 위해.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-yaml python3-pytest \
 && rm -rf /var/lib/apt/lists/*

# Kafka 커넥터는 이미지를 만들 때 넣는다. 실행할 때마다 `--packages` 로 내려받으면 시작이 느리고(네트워크 필요),
# 같은 이미지가 날마다 다른 jar 로 돌 수 있다. Spark 자신의 의존성 해석기(Ivy)로 한 번 풀어서 필요한 4개만 /opt/spark/jars 에 둔다.
# (Spark 가 이미 가진 lz4/snappy/zstd/slf4j 등은 복사하지 않는다 — 같은 클래스가 두 번 올라가는 것을 막는다.)
COPY docker/spark-base/resolve_packages.py /tmp/resolve_packages.py
RUN set -eux; \
    /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY}:${SPARK_VERSION} \
      --conf spark.jars.ivy=/tmp/ivy /tmp/resolve_packages.py; \
    cp /tmp/ivy/jars/org.apache.spark_spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
       /tmp/ivy/jars/org.apache.spark_spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
       /tmp/ivy/jars/org.apache.kafka_kafka-clients-*.jar \
       /tmp/ivy/jars/org.apache.commons_commons-pool2-*.jar \
       /opt/spark/jars/; \
    rm -rf /tmp/ivy /tmp/resolve_packages.py /root/.ivy2* /root/.m2; \
    ls /opt/spark/jars | grep -E 'spark-sql-kafka|spark-token-provider-kafka|kafka-clients|commons-pool2'

# 컨테이너 안에서 테스트: pyspark/py4j 경로를 맞춰서 pytest 를 실행하는 래퍼 (scripts/spark_test.sh 가 사용)
COPY docker/spark-base/urbanstream-pytest.sh /usr/local/bin/urbanstream-pytest
RUN chmod 0755 /usr/local/bin/urbanstream-pytest

# 우리 코드. 데이터(/data)는 이미지에 넣지 않고 compose 가 마운트한다.
WORKDIR /app
COPY src /app/src
COPY streaming /app/streaming
COPY batch /app/batch
COPY config /app/config
COPY pytest.ini /app/pytest.ini
ENV PYTHONPATH=/app/src:/app \
    PYTHONDONTWRITEBYTECODE=1

# 공식 이미지의 실행 사용자 (uid 185)로 돌아간다 — 루트로 실행하지 않는다.
USER spark
