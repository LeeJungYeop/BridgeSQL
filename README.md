# Semantic Query Engine

> 지능형 자연어 데이터베이스 인터페이스 - Zero-Config, AI Semantic Catalog, Interactive Clarification

## 🚀 Quick Start

### 1. 설치
```bash
# 가상환경 생성
python -m venv .venv
.venv\Scripts\activate  # Windows

# 의존성 설치
pip install -e ".[dev]"
```

### 2. 환경 설정
```bash
# .env 파일 생성
copy .env.example .env

# .env 파일 편집하여 API 키와 DB 정보 입력
```

### 3. DB 프로파일링
```bash
# DB 연결 후 자동으로 스키마 분석 및 세만틱 카탈로그 생성
sqe profile
```

### 4. 자연어 쿼리
```bash
# 테이블/컬럼 검색
sqe search "고객 이메일"

# 자연어로 SQL 생성 및 실행
sqe query "지난 달에 가장 많이 팔린 상품 10개"
```

## 📁 프로젝트 구조

```
src/semantic_engine/
├── config.py              # 환경 설정
├── cli.py                 # CLI 인터페이스
├── profiler/              # DB 스키마 분석
│   ├── connector.py       # DB 연결 관리
│   ├── schema_extractor.py # 메타데이터 추출
│   └── sampler.py         # 샘플 데이터 수집
├── semantic/              # 세만틱 카탈로그
│   ├── generator.py       # LLM 기반 설명 생성
│   └── catalog.py         # 카탈로그 저장/조회
├── rag/                   # 벡터 검색
│   └── retriever.py       # ChromaDB 기반 검색
├── engine/                # 쿼리 엔진
│   ├── intent_guard.py    # 모호성 판단
│   ├── sql_generator.py   # SQL 생성
│   └── validator.py       # SQL 검증
└── mcp/                   # MCP 서버 (Phase 3)
    └── server.py
```

## 🔑 환경 변수

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `GEMINI_API_KEY` | Google Gemini API 키 | (필수) |
| `DATABASE_URL` | DB 연결 문자열 | - |
| `CHROMA_PERSIST_DIR` | ChromaDB 저장 경로 | `./data/chroma` |

## 📜 License

MIT
