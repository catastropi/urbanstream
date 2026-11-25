# UrbanStream 수집기 이미지 (STEP 3A). 결정 근거: docs/adr/0004-collector-topics.md (컨테이너로 옮긴 이유는 같은 문서의 '변경' 절)
# python slim 위에 requirements-collector.txt 만 설치하고 collector/, src/, config/ 만 복사한다.
# API 키는 이미지에 넣지 않는다. compose 가 실행할 때 환경 변수로 두 개(SEOUL_API_KEY, DATA_GO_KR_KEY)만 넘긴다.
# 원본 응답(data/raw)과 호출 예산(data/collector/budget.json)은 호스트 폴더를 마운트해서 호스트에서 돌던 때와 같은 파일을 이어 쓴다.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src:/app

WORKDIR /app
COPY requirements-collector.txt ./
RUN pip install -r requirements-collector.txt

COPY src ./src
COPY collector ./collector
COPY config ./config

# 일반 사용자로 실행한다. 쓰는 곳은 마운트한 /app/data 아래뿐이다.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin collector
USER 10001

CMD ["python", "-m", "collector", "run"]
