# BridgeSQL

> 자연어로 데이터베이스에 질문하는 NL2SQL 플랫폼 — AI가 데이터 정의서를 자동 생성하고, MCP를 통해 AI 에이전트와 직접 연결됩니다.

---

## NL2SQL이란?

자연어(일상적인 말)를 데이터베이스 질의 언어인 SQL로 자동 변환해 주는 기술입니다. 사용자가 복잡한 데이터베이스 구조나 SQL 문법을 몰라도, 일상 언어로 질문하면 AI가 이를 이해하고 정확한 SQL 쿼리를 생성하여 데이터베이스에서 정보를 찾아줍니다.

## 기존 NL2SQL 방식의 문제점

일반적인 NL2SQL은 테이블명, 컬럼명, 데이터 타입 등의 단순 물리적 스키마 정보를 프롬프트에 주입하는 방식을 취합니다. 하지만 실제 산업 현장에서는 물리적 명칭과 실제 비즈니스 의미 사이의 불일치 문제가 빈번히 발생합니다.

예를 들어, DB에는 `ST_CD`라는 컬럼이 존재하지만, 현업 사용자는 이를 `"장비 상태"` 혹은 `"가동 여부"`라고 부릅니다. LLM이 이 두 단어가 같은 대상을 가리킨다는 의미적 연결 정보가 없다면, 사용자의 질문을 정확한 쿼리로 변환할 수 없습니다.

## BridgeSQL이 해결하는 방법

BridgeSQL은 DB에 연결하면 LLM이 각 테이블과 컬럼의 비즈니스 의미를 분석하여 자동으로 **데이터 정의서**를 생성합니다.

|  | As-is | To-be |
|---|---|---|
| 컬럼명 | 정상여부 | 정상여부 |
| 데이터 타입 | BIGINT | BIGINT |
| 대푯값 | 0, 1 | 0, 1 |
| 비즈니스명 | — | 이미지 불량 여부 |
| 설명 | — | 해당 이미지가 불량인지 여부. 0=정상, 1=불량 |
| 검색 키워드 | — | 이미지, 불량, 정상, 비정상, 검사, 상태 |

생성된 데이터 정의서는 한국어 임베딩 모델(`jhgan/ko-sroberta-multitask`)을 통해 벡터로 변환되어 ChromaDB에 저장됩니다. 컬럼과 테이블을 각각 별도로 임베딩하며, 물리 정보(테이블명·컬럼명·데이터 타입·샘플 데이터)에 LLM이 생성한 비즈니스명·설명·키워드가 함께 포함됩니다. 사용자 질문이 입력되면 동일 모델로 임베딩한 뒤 코사인 유사도 기반 검색을 수행하고, 컬럼 점수에 부모 테이블 점수를 합산하는 Score Fusion을 적용하여 최종 컨텍스트를 LLM에 전달합니다.

```
[인덱싱] sqe profile 실행 시
─────────────────────────────────────────
[입력] 데이터베이스 연결
    │
    ▼
[Step 1] 데이터 정의서 생성
    IN  : 테이블명·컬럼명·데이터 타입·제약조건·샘플 데이터
    처리 : LLM이 비즈니스명·설명·검색 키워드 자동 생성
    OUT : 컬럼별 비즈니스명 / 설명 / 키워드
    │
    ▼
[Step 2] 벡터 임베딩 및 저장
    컬럼 : 테이블명·컬럼명·데이터 타입·비즈니스명·설명·키워드·샘플값
    테이블 : 테이블명·비즈니스명·설명·컬럼 목록
    모델 : jhgan/ko-sroberta-multitask
    OUT : ChromaDB 저장 (cosine space)


[검색] retrieve_context 호출 시
─────────────────────────────────────────
[입력] 사용자 자연어 질문
    │
    ▼
[Step 1] 질문 임베딩
    IN  : 사용자 질문
    모델 : jhgan/ko-sroberta-multitask
    OUT : 질문 벡터
    │
    ▼
[Step 2] 유사도 검색 + Score Fusion
    IN  : 질문 벡터
    방법 : 코사인 유사도 top-k → 컬럼 점수 + 부모 테이블 점수 합산
    OUT : 관련 컬럼·테이블 컨텍스트
    │
    ▼
[출력] 컨텍스트 → generate_sql
```

예를 들어, `"지난주에 찍힌 사진 중에서 통과된 데이터만 골라내줘"`라는 질문이 들어오면, DB에는 `PASS_YN`이나 `INSP_RESULT` 같은 컬럼명만 존재하더라도, 해당 컬럼의 비즈니스명(`합격 여부`)·설명(`검사 결과. P=합격, F=불합격`)·키워드(`통과, 합격, 불량, 검사`)가 임베딩에 포함되어 있기 때문에 유사도가 높게 계산됩니다. `IMG_DT`(촬영 일시) 컬럼도 키워드(`날짜, 촬영, 일시`)를 통해 함께 검색되어 프롬프트에 포함됩니다.

## Memory & Few-shot

BridgeSQL은 사용자가 결과를 확인한 질문-SQL 쌍을 메모리에 저장하고, 이후 유사한 질문이 들어오면 저장된 사례를 few-shot 예시로 LLM에 제공합니다. 예시가 쌓일수록 유사 패턴의 질문에 더 정확하게 대응할 수 있는 구조입니다.

예를 들어, 과거에 `"지난주에 찍힌 사진 중에서 통과된 데이터만 골라내줘"`에 대해 `정상여부 = 0` 기준으로 SQL 생성에 성공했다면, 이후 `"어제 찍은 것 중에서 쓸만한 사진만 보여줘"`와 같이 유사한 질문이 들어왔을 때 해당 사례가 few-shot으로 활용됩니다.

---

## MCP 아키텍처

MCP(Model Context Protocol)는 Anthropic이 제안한 개방형 표준으로, AI 에이전트가 외부 도구·데이터와 구조화된 방식으로 통신할 수 있게 해주는 인터페이스입니다.

BridgeSQL은 MCP의 세 가지 구성요소를 모두 활용하여 에이전트가 상황에 따라 스스로 경로를 선택할 수 있는 구조로 설계되었습니다.

| 구성요소 | 예시 | 특징 |
|---|---|---|
| Tools | `retrieve_context`, `generate_sql` 등 | 동적 입력, 호출마다 결과 다름 |
| Resources | `bridgesql://schema`, `bridgesql://table/{name}` | URI 고정, 파라미터 없음 |
| Prompts | `bridgesql-instructions` | 에이전트 행동 지침 |

### Tools

에이전트가 직접 호출하여 파이프라인을 구성하는 핵심입니다. 각 도구는 단일 책임만 가지며, 에이전트는 상황에 따라 이를 조합하여 사용합니다.

| 도구 | 역할 |
|---|---|
| `search_examples` | 유사한 과거 사례 검색 |
| `retrieve_context` | RAG로 질문과 관련된 테이블·컬럼 컨텍스트 검색 |
| `generate_sql` | SQL 생성 (실행하지 않음) |
| `execute_sql` | SQL 실행 |
| `learn` | 확인된 질문-SQL 쌍을 메모리에 저장 |
| `examples_delete` | 저장된 예제 삭제 |
| `catalog_edit` | 데이터 정의서 수정 |

### Resources

에이전트가 데이터베이스 구조를 파악할 때 참조하는 정적 정보입니다.

- `bridgesql://schema` → 전체 테이블 목록과 요약
- `bridgesql://table/{테이블명}` → 특정 테이블의 컬럼·타입·비즈니스명·샘플값

### Prompts

`bridgesql-instructions`에는 에이전트의 행동 규칙이 담겨 있습니다.

- 숫자가 포함된 답변 전 모순 여부 자기 검증
- SQL 실패 시 오류 메시지를 담아 `generate_sql` 재호출
- 사용자가 결과를 긍정적으로 확인했을 때만 `learn` 호출 제안 (동의 없이 자동 저장 금지)

### 동작 방식

1. `search_examples`로 유사한 과거 사례가 있는지 확인합니다.
    1. 사례가 있으면 → 저장된 SQL을 `execute_sql`로 바로 재활용
    2. 사례가 없으면 → `retrieve_context`로 관련 컬럼 컨텍스트를 가져옴

2. `generate_sql`로 SQL을 생성하고 `execute_sql`로 실행합니다.
    1. 실행이 실패하면 → 오류 메시지를 `error_context`에 담아 `generate_sql`을 재호출하여 보정된 SQL을 얻음
    2. 실행이 성공하고 사용자가 결과를 긍정적으로 확인하면 → 에이전트가 `learn` 호출을 제안하며, 사용자가 동의할 경우에만 해당 질문-SQL 쌍이 메모리에 저장되어 이후 유사 질문의 few-shot 예시로 활용됨

---

## 설치

```bash
pip install bridgesql
```

또는 uvx로 바로 실행:

```bash
uvx bridgesql init
```

## CLI 사용법

```
sqe init                      최초 설정 (API 키, DB 연결 정보)
sqe config                    현재 설정 확인
sqe profile                   스키마 분석 및 데이터 정의서 생성
sqe status                    현재 상태 확인

sqe catalog show              데이터 정의서 조회
sqe catalog edit <table>      테이블 설명 수정
sqe catalog clear             데이터 정의서 초기화

sqe examples list             저장된 예제 목록
sqe examples add              예제 추가
sqe examples delete <id>      예제 삭제
sqe examples clear            모든 예제 삭제
```

## MCP 연결

### Claude Desktop

`claude_desktop_config.json`에 추가:

```json
{
  "mcpServers": {
    "bridgesql": {
      "command": "uvx",
      "args": ["bridgesql"],
      "env": {
        "GEMINI_API_KEY": "your-api-key",
        "DATABASE_URL": "mysql+pymysql://user:password@host/dbname"
      }
    }
  }
}
```

### Claude Code

```bash
claude mcp add bridgesql uvx bridgesql \
  -e GEMINI_API_KEY=your-api-key \
  -e DATABASE_URL=mysql+pymysql://user:password@host/dbname
```

## 환경 변수

| 변수 | 설명 | 기본값 |
|---|---|---|
| `GEMINI_API_KEY` | Google Gemini API 키 | — |
| `DATABASE_URL` | SQLAlchemy 연결 문자열 | — |
| `DB_HOST` | DB 호스트 | `localhost` |
| `DB_PORT` | DB 포트 | `3306` |
| `DB_USER` | DB 사용자 | `root` |
| `DB_PASSWORD` | DB 비밀번호 | — |
| `DB_NAME` | DB 이름 | — |

## 프로젝트 구조

```
bridgesql/
├── server.py          MCP 서버 진입점
├── cli.py             CLI 진입점
├── config.py          환경 설정
├── state.py           공유 상태 (connector, retriever, schema)
├── formatters.py      텍스트 포맷 헬퍼
├── tools/             MCP Tools
│   ├── context.py     retrieve_context
│   ├── sql.py         generate_sql, execute_sql
│   ├── examples.py    search_examples, learn, examples_delete
│   └── catalog.py     catalog_edit
├── resources/         MCP Resources
├── prompts/           MCP Prompts
├── db/                DB 연결·스키마 추출·샘플링
├── rag/               벡터 검색 (ChromaDB)
├── semantic/          데이터 정의서 생성·저장
└── engine/            SQL 생성·검증
```

