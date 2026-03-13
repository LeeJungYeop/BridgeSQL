FROM python:3.11-slim

WORKDIR /app

# 시스템 의존성
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 패키지 설치
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

# 데이터 디렉토리 생성
RUN mkdir -p data/chroma data/catalog data/benchmark data/conversation data/feedback

# 환경변수 (기업이 docker run -e 로 오버라이드)
ENV DB_HOST=localhost
ENV DB_PORT=3306
ENV DB_USER=root
ENV DB_PASSWORD=password
ENV DB_NAME=mydb
ENV GEMINI_API_KEY=your_key_here
ENV GEMINI_MODEL=gemini-2.0-flash
ENV CHROMA_PERSIST_DIR=/app/data/chroma
ENV CHROMA_COLLECTION_NAME=semantic_catalog
ENV LOG_LEVEL=INFO
ENV SAMPLE_DATA_LIMIT=10
ENV MAX_SQL_RETRY=3

# 볼륨 (카탈로그/ChromaDB 영속성)
VOLUME ["/app/data"]

# 진입점 스크립트
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
