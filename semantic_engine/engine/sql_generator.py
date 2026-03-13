"""
Text-to-SQL 생성기
자연어 질문을 SQL 쿼리로 변환하고 Self-Correction 기능을 제공합니다.
"""

import json
from dataclasses import dataclass

from semantic_engine.config import get_settings
import google.generativeai as genai


@dataclass
class SQLResult:
    """SQL 생성 결과"""
    sql: str
    explanation: str
    tables_used: list[str]
    confidence: float
    retry_count: int = 0


class SQLGenerator:
    """Text-to-SQL 생성기"""
    
    GENERATION_PROMPT = """당신은 자연어를 SQL로 변환하는 전문가입니다.
주어진 스키마 정보와 질문을 기반으로 정확한 SQL을 생성하세요.

## 데이터베이스 스키마
{schema}

## 관련 컨텍스트
{context}

{few_shot_context}

## 사용자 질문
"{question}"

## 규칙
1. SELECT 쿼리만 생성 (INSERT, UPDATE, DELETE 불가)
2. 테이블/컬럼명은 백틱(`)으로 감싸기
3. 필요시 JOIN 사용
4. 결과는 적절히 정렬하고 LIMIT 적용 고려
5. 집계 함수 사용 시 GROUP BY 확인

## 출력 형식 (JSON)
{{
    "sql": "생성된 SQL 쿼리",
    "explanation": "이 쿼리가 질문에 어떻게 답하는지 설명",
    "tables_used": ["사용된", "테이블", "목록"],
    "confidence": 0.0-1.0
}}

JSON만 출력하세요:"""
    
    CORRECTION_PROMPT = """SQL 실행 중 오류가 발생했습니다. 수정된 SQL을 생성하세요.

## 원본 SQL
```sql
{original_sql}
```

## 오류 메시지
{error_message}

## 스키마 정보
{schema}

## 수정 지침
1. 오류 원인을 분석하세요
2. 테이블/컬럼명 철자 확인
3. 데이터 타입 호환성 확인
4. 문법 오류 수정

## 출력 형식 (JSON)
{{
    "sql": "수정된 SQL 쿼리",
    "explanation": "무엇을 수정했는지 설명",
    "tables_used": ["사용된", "테이블", "목록"],
    "confidence": 0.0-1.0
}}

JSON만 출력하세요:"""
    
    def __init__(self):
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key.get_secret_value())
        self.model = genai.GenerativeModel(settings.gemini_model)
        self.max_retries = settings.max_sql_retry
    
    async def generate(
        self, 
        question: str, 
        schema: str, 
        context: str,
        few_shots: list[dict] | None = None
    ) -> SQLResult:
        """자연어 질문에서 SQL 생성"""
        
        few_shot_context = ""
        if few_shots:
            few_shot_context = "## 참고 예시 (Past Successes)\n"
            for i, example in enumerate(few_shots, 1):
                few_shot_context += f"예시 {i}:\n"
                few_shot_context += f"질문: {example['question']}\n"
                few_shot_context += f"SQL: ```sql\n{example['sql']}\n```\n\n"
        
        prompt = self.GENERATION_PROMPT.format(
            schema=schema,
            context=context,
            few_shot_context=few_shot_context,
            question=question,
        )
        
        response = await self.model.generate_content_async(prompt)
        return self._parse_response(response.text)
    
    async def correct(
        self, 
        original_sql: str, 
        error_message: str,
        schema: str,
        retry_count: int = 0,
    ) -> SQLResult:
        """오류 발생 시 SQL 자동 수정"""
        
        if retry_count >= self.max_retries:
            raise ValueError(f"최대 재시도 횟수({self.max_retries}) 초과")
        
        prompt = self.CORRECTION_PROMPT.format(
            original_sql=original_sql,
            error_message=error_message,
            schema=schema,
        )
        
        response = await self.model.generate_content_async(prompt)
        result = self._parse_response(response.text)
        result.retry_count = retry_count + 1
        
        return result
    
    def _parse_response(self, text: str) -> SQLResult:
        """LLM 응답 파싱"""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1])
        
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # JSON 파싱 실패 - 텍스트에서 SQL 추출 시도
            return SQLResult(
                sql="",
                explanation="SQL 생성 실패",
                tables_used=[],
                confidence=0.0,
            )
        
        return SQLResult(
            sql=data.get("sql", ""),
            explanation=data.get("explanation", ""),
            tables_used=data.get("tables_used", []),
            confidence=data.get("confidence", 0.0),
        )


def format_schema_tables_only(schema_info) -> str:
    """테이블명만 반환 (RAG-only 모드용 — 컬럼 상세는 RAG context로 대체)"""
    parts = []
    for table in schema_info.tables:
        desc = f"## {table.name}"
        if table.business_name:
            desc += f" ({table.business_name})"
        parts.append(desc)
    return "\n".join(parts)


def format_schema_raw(schema_info) -> str:
    """원시 스키마만 (컬럼명 + 타입 + PK/FK). 세만틱 정보 없음 — Baseline용"""
    parts = []
    for table in schema_info.tables:
        columns = []
        for col in table.columns:
            col_str = f"  - `{col.name}` {col.data_type}"
            flags = []
            if col.is_primary_key:
                flags.append("PK")
            if col.is_foreign_key:
                flags.append(f"FK→{col.foreign_key_ref}")
            if not col.nullable:
                flags.append("NOT NULL")
            if flags:
                col_str += f" [{', '.join(flags)}]"
            columns.append(col_str)
        parts.append(f"## {table.name}\n" + "\n".join(columns))
    return "\n\n".join(parts)


def format_schema_for_prompt(schema_info) -> str:
    """스키마 정보를 프롬프트용 문자열로 변환"""
    parts = []
    
    for table in schema_info.tables:
        table_desc = f"## {table.name}"
        if table.business_name:
            table_desc += f" ({table.business_name})"
        if table.description:
            table_desc += f"\n{table.description}"
        
        columns = []
        for col in table.columns:
            col_str = f"  - `{col.name}` {col.data_type}"
            flags = []
            if col.is_primary_key:
                flags.append("PK")
            if col.is_foreign_key:
                flags.append(f"FK→{col.foreign_key_ref}")
            if not col.nullable:
                flags.append("NOT NULL")
            if flags:
                col_str += f" [{', '.join(flags)}]"
            if col.business_name:
                col_str += f" -- {col.business_name}"
            
            # 샘플 데이터 추가 (상위 5개 유니크 값)
            if col.sample_values:
                unique_samples = []
                for val in col.sample_values:
                    if val not in unique_samples:
                        unique_samples.append(val)
                    if len(unique_samples) >= 5:
                        break
                if unique_samples:
                    samples_str = ", ".join(str(v) for v in unique_samples)
                    col_str += f" (예: {samples_str})"
            
            columns.append(col_str)
        
        parts.append(table_desc + "\n" + "\n".join(columns))
    
    return "\n\n".join(parts)
