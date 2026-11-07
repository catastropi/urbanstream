# UrbanStream 대시보드 이미지 (Step 10). 결정 근거: docs/adr/0012-step10-dashboard.md
# 작게: python slim 위에 requirements-dashboard.txt 만 설치하고 dashboard/ 폴더만 복사한다. 테스트·문서·데이터·.env 는 .dockerignore 로 빠진다.
# 비밀(대시보드 API 키)은 이미지에 넣지 않는다 — 실행 때 환경 변수로만 받는다.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE}

# HOME=/tmp: 읽기 전용 파일시스템에서 streamlit 이 홈 아래에 쓰려고 하는 것을 tmpfs 로 돌린다.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    HOME=/tmp

WORKDIR /app
COPY requirements-dashboard.txt ./
RUN pip install -r requirements-dashboard.txt

COPY dashboard ./dashboard

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin dashuser
USER 10001

EXPOSE 8501
# 파일 감시 끔(읽기 전용·운영), 사용 통계 끔, 헤드리스. 주소는 컨테이너 안에서 0.0.0.0 — 밖으로는 compose 가 127.0.0.1 에만 연다.
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.fileWatcherType=none", "--browser.gatherUsageStats=false"]
