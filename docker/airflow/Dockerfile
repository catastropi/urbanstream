# UrbanStream Airflow 이미지 (Step 7). 공식 이미지 위에 "JDK + pyspark + DB 드라이버"를 얹는다. 결정 근거: docs/adr/0009-step7-airflow.md
#
# 왜 JDK 와 pyspark 를 넣나: gold_stage(Spark)를 Airflow 컨테이너 안에서 서브프로세스로 돌리기 위해서다(결정 ②-B).
#   docker.sock 을 마운트하는 방식(A)은 호스트 경로·권한·보안 문제로 쓰지 않는다.
# 버전은 모두 고정한다. pip 는 Airflow 공식 constraints 로 풀어 Airflow 의존성이 깨지지 않게 한다.
ARG AIRFLOW_IMAGE=apache/airflow:3.3.0-python3.12
FROM ${AIRFLOW_IMAGE}

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_MM=3.12
ARG PYSPARK_VERSION=4.1.3

USER root
# 베이스가 Debian 계열이라고 가정한다([미확인] — 빌드 로그로 확인). 17 이든 21 이든 Spark 4.1 이 돈다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home

USER airflow
ENV JAVA_HOME=/opt/java-home
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MM}.txt" \
      "psycopg[binary]>=3.2" "pyarrow>=15" pyyaml confluent-kafka "pyspark==${PYSPARK_VERSION}"
