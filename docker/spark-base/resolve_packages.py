"""이미지 빌드 중 한 번 실행: spark-submit --packages 가 Kafka 커넥터와 의존성을 내려받게 하는 최소 프로그램."""
from pyspark.sql import SparkSession

SparkSession.builder.master("local[1]").appName("resolve-packages").config("spark.ui.enabled", "false").getOrCreate().range(1).count()
