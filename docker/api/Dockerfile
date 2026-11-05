# UrbanStream API 이미지 (Step 9). 결정 근거: docs/adr/0011-step9-api.md
# 작게: python slim 위에 requirements-api.txt 만 설치하고 api/ 폴더만 복사한다. 테스트·문서·데이터·.env 는 .dockerignore 로 빠진다.
# 비밀(API 키·DB 비밀번호)은 이미지에 넣지 않는다 — 실행 때 환경 변수로만 받는다.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements-api.txt ./
RUN pip install -r requirements-api.txt

COPY api ./api

# 일반 사용자로 실행한다 (root 아님). 쓰기 가능한 곳이 필요 없다(compose 에서 읽기 전용 파일시스템 + /tmp 만 tmpfs).
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin apiuser
USER 10001

EXPOSE 8000
# 워커 1개: 속도 제한·캐시가 프로세스 안에 있다 (ADR-0011 D9-9). 접근 로그는 앱이 JSON 한 줄로 직접 남긴다.
CMD ["uvicorn", "api.main:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log", "--no-server-header"]
